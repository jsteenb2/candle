package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/etcd-io/bbolt"
	"github.com/influxdata/influxdb-client-go"
	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/internal/exchange/binance"
	"github.com/jsteenb2/candle/internal/exchange/coinbase"
	"github.com/jsteenb2/candle/internal/exchange/gemini"
	"github.com/jsteenb2/candle/internal/exchange/kraken"
)

func main() {
	ballast := make([]byte, 10<<30)
	var _ = ballast

	addr := flag.String("addr", envWithDefault("INFLUX_ADDR", "http://localhost:9999"), "address for influxdb; maps to env var INFLUX_ADDR")
	token := flag.String("token", "", "token for influxdb")
	bkt := flag.String("bucket", envWithDefault("INFLUX_BUCKET", "bucket1"), "influxdb bucket target; maps to env var INFLUX_BUCKET")
	user := flag.String("user", envWithDefault("INFLUX_USER", ""), "influxdb user; maps to env var INFLUX_USER")
	password := flag.String("password", envWithDefault("INFLUX_PASSWORD", ""), "influxdb password; maps to env var INFLUX_PASSWORD")
	org := flag.String("org", envWithDefault("INFLUX_ORG", "rg"), "influxdb org target; maps to env var INFLUX_ORG")
	dotPath := flag.String("dotpath", os.ExpandEnv("$HOME/.candled"), "path to boltdb")
	disableLive := flag.Bool("disable-live", os.Getenv("CANDLE_DISABLE_LIVE") == "true", "disable the live feed;")
	disableHistorical := flag.Bool("disable-historical", os.Getenv("CANDLE_DISABLE_HISTORICAL") == "true", "disable the historical feed;")
	historicalDuration := flag.Duration("historical-duration", envWithDefaultDur("CANDLE_HISTORICAL_DURATION", 30*24*time.Hour), "number of days to back fill, starting from now. ex: 24h translates to historical for most recent day")
	flag.Parse()

	if *addr == "" {
		log.Panic("no token provided")
	}
	if *bkt == "" {
		log.Panic("no bucket provided")
	}
	if *org == "" {
		log.Panic("no org provided")
	}
	if *dotPath == "" {
		log.Panic("no dot path provided")
	}

	if *token == "" {
		if t := getToken(); t != "" {
			*token = t
		}
	}

	{
		_, err := os.Stat(*dotPath)
		if err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(*dotPath, os.ModePerm); err != nil {
					log.Panic(err)
				}
			} else {
				log.Panic(err)
			}
		}

	}

	db, err := bbolt.Open(path.Join(*dotPath, "candles.db"), os.ModePerm, bbolt.DefaultOptions)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	lastSeenRepo, err := exchange.NewLastSeenRepo(db)
	if err != nil {
		log.Panic(err)
	}

	hc := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 5 * time.Second,
		},
		Timeout: time.Minute,
	}

	influxOpts := []influxdb.Option{influxdb.WithHTTPClient(hc)}
	if *user != "" && *password != "" {
		influxOpts = append(influxOpts, influxdb.WithUserAndPass(*user, *password))
	}

	iw := exchange.NewInfluxWriter(mustNewInfluxC(*addr, *token, influxOpts...), *bkt, *org)
	if err := iw.Ping(context.Background(), 15); err != nil {
		log.Panic(fmt.Errorf("failed to ping influxdb: %w", err))
	}
	if *token == "" {
		res, err := iw.Setup(context.Background())
		if err != nil {
			log.Panic(err)
		}
		mustWriteToken(*dotPath, res.Auth.Token)
		iw = exchange.NewInfluxWriter(mustNewInfluxC(*addr, res.Auth.Token, influxOpts...), *bkt, *org)
	}

	binanceC, err := binance.New()
	if err != nil {
		log.Panic(err)
	}

	coinbaseC, err := coinbase.New()
	if err != nil {
		log.Panic(err)
	}

	geminiC, err := gemini.New()
	if err != nil {
		log.Panic(err)
	}

	krakenC, err := kraken.New()
	if err != nil {
		log.Panic(err)
	}

	exchanges := []exchange.Exchange{
		binanceC,
		coinbaseC,
		geminiC,
		krakenC,
	}

	exchangeRunner := exchange.Runner{
		InfluxC:   iw,
		Repo:      lastSeenRepo,
		Exchanges: exchanges,
	}

	pairs := []exchange.Pair{
		exchange.XBTUSD,
		exchange.XBTEUR,
		exchange.BCHUSD,
		exchange.BCHEUR,
		exchange.ETHUSD,
		exchange.ETHEUR,
		exchange.ETCUSD,
		exchange.ETCEUR,
		exchange.XMRUSD,
		exchange.XMREUR,
		exchange.XRPUSD,
		exchange.XRPEUR,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := new(sync.WaitGroup)

	if !*disableLive {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := exchangeRunner.Live(ctx, pairs); err != nil {
				log.Println("live err: ", err)
			}
		}()
	}

	if !*disableHistorical {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := exchangeRunner.BackFill(ctx, pairs, time.Now().Add(-*historicalDuration))
			if err != nil {
				log.Println("backfill err: ", err)
			}
		}()
	}
	done := make(chan os.Signal, 2)
	signal.Notify(done, os.Interrupt, os.Kill)

	<-done
	cancel()
	wg.Wait()
}

func getToken() string {
	if t := os.Getenv("INFLUX_TOKEN"); t != "" {
		return t
	}

	home := os.Getenv("$HOME")
	if home == "" {
		home = "/root"
	}

	b, err := ioutil.ReadFile(filepath.Join(home, "/.candled/credentials"))
	if err != nil {
		time.Sleep(time.Second)
		return ""
	}
	return string(bytes.TrimSpace(b))
}

func mustWriteToken(dotPath, token string) {
	if token == "" {
		log.Panic("expected a valid token but got empty string")
	}
	err := ioutil.WriteFile(path.Join(dotPath, "credentials"), []byte(token), os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
}

func mustNewInfluxC(addr, token string, opts ...influxdb.Option) *influxdb.Client {
	influxC, err := influxdb.New(addr, token, opts...)
	if err != nil {
		log.Panic(err)
	}
	return influxC
}

func envWithDefault(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func envWithDefaultDur(key string, defaultValue time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		d, err := time.ParseDuration(val)
		if err != nil {
			log.Panic("invalid duration provided: " + err.Error())
		}
		return d
	}
	return defaultValue
}
