package exchange

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/influxdata/influxdb-client-go"
	"golang.org/x/time/rate"
)

type InfluxClient struct {
	client      *influxdb.Client
	readLimiter *rate.Limiter
	bkt         string
	org         string
}

func NewInfluxWriter(influxC *influxdb.Client, bkt, org string) *InfluxClient {
	return &InfluxClient{
		client:      influxC,
		readLimiter: rate.NewLimiter(10, 5),
		bkt:         bkt,
		org:         org,
	}
}

func (w *InfluxClient) Ping(ctx context.Context, maxTrys int) error {
	if maxTrys <= 0 {
		maxTrys = 1
	}
	for i := 1; i <= maxTrys; i++ {
		err := w.client.Ping(ctx)
		if err == nil {
			break
		}
		log.Println(i, "failed to ping influxdb")

		if i == maxTrys {
			return err
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return nil
}

func (w *InfluxClient) Write(ctx context.Context, metrics []influxdb.Metric) error {
	if len(metrics) == 0 {
		return errors.New("no metrics provided")
	}

	_, err := w.client.Write(ctx, w.bkt, w.org, metrics...)
	return err
}

type FindOpts struct {
	Exchange  string
	Pair      Pair
	Start     time.Time
	End       time.Time
	Threshold time.Duration
	Interval  time.Duration
}

func (w *InfluxClient) FindWindow(ctx context.Context, opt FindOpts) (time.Time, time.Time, error) {
	if err := w.readLimiter.Wait(ctx); err != nil {
		return time.Time{}, time.Time{}, err
	}

	if opt.Threshold == 0 {
		opt.Threshold = 5*time.Hour - 2*time.Minute
	}
	if opt.Interval == 0 {
		opt.Interval = time.Minute
	}

	st := opt.Start
	end := opt.End
	if opt.End.UnixNano()-opt.Start.UnixNano() > 24*int64(time.Hour) {
		st = opt.End.Add(-24*time.Hour - 2*time.Minute)
	}

	const queryFmt = `
from(bucket: "%s")
	|> range(start: -%dns, stop: -%dns)
	|> filter(fn : (r) =>
		r.exchange == %q and
		r._measurement == %q and
		r.cur == %q
	)
	|> aggregateWindow(every: %s, fn: count)
	|> filter(fn: (r) => r._value == 0)
	|> sort(columns: ["_time"], desc: true)
	|> keep(columns: ["_time"])
	|> limit(n: 500)
`
	query := fmt.Sprintf(queryFmt,
		w.bkt,
		time.Since(st),
		time.Since(end),
		opt.Exchange,
		opt.Pair.Market(),
		opt.Pair.Currency(),
		opt.Interval,
	)

	res, err := w.client.QueryCSV(ctx, query, w.org)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	defer res.Close()

	var (
		cur  time.Time
		seen bool
	)
	for res.Next() {
		if len(res.Row) < 4 {
			continue
		}

		cur, err = time.Parse(time.RFC3339, res.Row[3])
		if err != nil {
			continue
		}

		if !seen {
			end = cur
			seen = true
		}
		if end.UnixNano()-cur.UnixNano() > int64(opt.Threshold) {
			break
		}
		st = cur
	}
	if cur.IsZero() {
		return st, time.Time{}, nil
	}

	return st, end, nil
}
