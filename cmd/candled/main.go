package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"time"

	"github.com/etcd-io/bbolt"
	"github.com/influxdata/influxdb-client-go"
	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/internal/exchange/binance"
	"github.com/jsteenb2/candle/internal/exchange/coinbase"
)

func main() {
	ballast := make([]byte, 10<<30)
	var _ = ballast

	addr := flag.String("addr", "http://localhost:9999", "address for influxdb")
	token := flag.String("token", os.Getenv("INFLUX_TOKEN"), "token for influxdb")
	bkt := flag.String("bucket", "bucket1", "influxdb bucket target")
	org := flag.String("org", os.Getenv("INFLUX_ORG"), "influxdb org target")
	dotPath := flag.String("dotpath", os.ExpandEnv("$HOME/.candled"), "path to boltdb")
	flag.Parse()

	if *addr == "" {
		log.Panic("no token provided")
	}
	if *token == "" {
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

	influxC, err := influxdb.New(*addr, *token, influxdb.WithHTTPClient(hc))
	if err != nil {
		log.Panic(err)
	}

	iw := exchange.NewInfluxWriter(influxC, *bkt, *org)
	if err := iw.Ping(context.Background(), 15); err != nil {
		log.Panic(fmt.Errorf("failed to ping influxdb: %w", err))
	}

	binanceC, err := binance.New()
	if err != nil {
		log.Panic(err)
	}

	coinbaseC, err := coinbase.New()
	if err != nil {
		log.Panic(err)
	}

	exchanges := []exchange.Exchange{
		binanceC,
		coinbaseC,
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
	type exchangeStream struct {
		pair   exchange.Pair
		stream <-chan exchange.Metrics
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now().Add(-180 * 24 * time.Hour)

	streams := make([]exchangeStream, 0, len(pairs)*len(exchanges))
	for _, p := range pairs {
		readStreams, err := exchangeRunner.BackFill(ctx, p, start)
		if err != nil {
			log.Println("client err: ", err)
			continue
		}

		for s := range readStreams {
			streams = append(streams, exchangeStream{
				pair:   p,
				stream: s,
			})
		}
	}

	type mergedStream struct {
		pair exchange.Pair
		exM  exchange.Metrics
	}
	merged := make(chan mergedStream)
	wg := new(sync.WaitGroup)
	for _, st := range streams {
		wg.Add(1)
		go func(ex exchangeStream) {
			defer wg.Done()
			for m := range ex.stream {
				merged <- mergedStream{
					pair: ex.pair,
					exM:  m,
				}
			}
		}(st)
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	done := make(chan os.Signal, 2)
	signal.Notify(done, os.Interrupt, os.Kill)

	for {
		select {
		case <-done:
			cancel()
			return
		case m, ok := <-merged:
			if !ok {
				return
			}
			if err := iw.Write(context.Background(), m.exM.Metrics); err != nil {
				log.Printf("failed to write to influxdb for market=%s cur=%s: err=%s", m.pair.Market(), m.pair.Currency(), err)
				continue
			}
			log.Printf("write successful: exchange=%s market=%s cur=%s batch_size=%d start=%q stop=%q spans=%s",
				m.exM.Exchange,
				m.pair.Market(),
				m.pair.Currency(),
				len(m.exM.Metrics),
				m.exM.Start.Format(time.Stamp),
				m.exM.End.Format(time.Stamp),
				time.Duration(m.exM.End.UnixNano()-m.exM.Start.UnixNano()),
			)
		}
	}
}
