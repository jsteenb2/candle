package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/jsteenb2/candle/internal/exchange/binance"

	"github.com/etcd-io/bbolt"
	"github.com/influxdata/influxdb-client-go"
	"github.com/jsteenb2/candle/internal/exchange"
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

	lastSeenRepo, err := newLastSeenRepo(db)
	if err != nil {
		log.Panic(err)
	}

	influxC, err := influxdb.New(*addr, *token)
	if err != nil {
		log.Panic(err)
	}

	maxTrys := 15
	for i := 1; i <= maxTrys; i++ {
		err := influxC.Ping(context.Background())
		if err == nil {
			break
		}
		log.Println(i, "failed to ping influxdb")

		if i == maxTrys {
			log.Panic(fmt.Errorf("failed to ping influxdb: %w", err))
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}

	binanceC, err := binance.New()
	if err != nil {
		log.Panic(err)
	}

	//coinbaseC, err := coinbase.New()
	//if err != nil {
	//	log.Panic(err)
	//}

	//krakenC, err := kraken.New()
	//if err != nil {
	//	log.Panic(err)
	//}

	exchanges := []exchange.Exchange{
		binanceC,
		//coinbaseC,
		//krakenC,
	}

	exchangeRunner := exchange.Runner{
		InfluxC:   influxC,
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

	start := time.Now().Add(-7 * 24 * time.Hour)

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
		pair            exchange.Pair
		exchangeMetrics exchange.Metrics
	}
	merged := make(chan mergedStream)
	wg := new(sync.WaitGroup)
	for _, st := range streams {
		wg.Add(1)
		go func(ex exchangeStream) {
			defer wg.Done()
			for m := range ex.stream {
				merged <- mergedStream{
					pair:            ex.pair,
					exchangeMetrics: m,
				}
			}
		}(st)
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	iw := newInfluxWriter(influxC, *bkt, *org)

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
			if err := iw.Write(context.Background(), m.exchangeMetrics.Metrics); err != nil {
				log.Printf("failed to write to influxdb for market=%s cur=%s: err=%s", m.pair.Market(), m.pair.Currency(), err)
				continue
			}
			log.Printf("write successful: exchange=%s market=%s cur=%s batch_size=%d", m.exchangeMetrics.Exchange, m.pair.Market(), m.pair.Currency(), len(m.exchangeMetrics.Metrics))
		}
	}
}

type influxWriter struct {
	client *influxdb.Client
	bkt    string
	org    string
}

func newInfluxWriter(influxC *influxdb.Client, bkt, org string) *influxWriter {
	return &influxWriter{
		client: influxC,
		bkt:    bkt,
		org:    org,
	}
}

func (w *influxWriter) Write(ctx context.Context, metrics []influxdb.Metric) error {
	if len(metrics) == 0 {
		return errors.New("no metrics provided")
	}

	_, err := w.client.Write(ctx, w.bkt, w.org, metrics...)
	return err
}

type lastSeenRepo struct {
	db *bbolt.DB
}

func newLastSeenRepo(db *bbolt.DB) (*lastSeenRepo, error) {
	l := &lastSeenRepo{db: db}

	if err := l.init(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *lastSeenRepo) GetLast(key string) time.Time {
	var last time.Time
	l.db.View(func(tx *bbolt.Tx) error {
		val := tx.
			Bucket(l.bucket()).
			Get([]byte(key))
		i, err := strconv.ParseInt(string(val), 10, 64)
		if err == nil {
			last = time.Unix(0, i)
		}
		return err
	})
	return last
}

func (l *lastSeenRepo) PutLast(key string, val time.Time) error {
	return l.db.Update(func(tx *bbolt.Tx) error {
		return tx.
			Bucket(l.bucket()).
			Put([]byte(key), []byte(strconv.FormatInt(val.UnixNano(), 10)))
	})
}

func (l *lastSeenRepo) bucket() []byte {
	return []byte("last_seen")
}

func (l *lastSeenRepo) init() error {
	return l.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(l.bucket())
		return err
	})
}
