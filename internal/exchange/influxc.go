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

func (w *InfluxClient) Setup(ctx context.Context) (*influxdb.SetupResult, error) {
	return w.client.Setup(ctx, w.bkt, w.org, 0)
}

func (w *InfluxClient) Write(ctx context.Context, metrics []influxdb.Metric) error {
	if len(metrics) == 0 {
		return errors.New("no metrics provided")
	}

	_, err := w.client.Write(ctx, w.bkt, w.org, metrics...)
	return err
}

type BatchWriter struct {
	name     string
	maxSize  int
	interval time.Duration

	influxC InfluxC

	metricsStream chan influxdb.Metric
	sem           chan struct{}
}

func NewBatchWriter(ctx context.Context, name string, influxC InfluxC, maxSize, numWriters int, flushInterval time.Duration) *BatchWriter {
	bw := &BatchWriter{
		name:          name,
		influxC:       influxC,
		maxSize:       maxSize,
		metricsStream: make(chan influxdb.Metric),
		sem:           make(chan struct{}, numWriters),
		interval:      flushInterval,
	}
	go bw.Run(ctx)
	return bw
}

func (bw *BatchWriter) Write(ctx context.Context, metrics ...influxdb.Metric) {
	for _, m := range metrics {
		select {
		case <-ctx.Done():
		case bw.metricsStream <- m:
		}
	}
}

func (bw *BatchWriter) Run(ctx context.Context) {
	write := func(metrics []influxdb.Metric) {
		if len(metrics) == 0 {
			return
		}

		batch := make([]influxdb.Metric, len(metrics))
		copy(batch, metrics)

		bw.sem <- struct{}{}
		go func() {
			defer func() { <-bw.sem }()

			start := time.Now()
			if err := bw.influxC.Write(ctx, batch); err != nil {
				log.Printf("%s failed to write: %s", bw.name, err)
				return
			}
			log.Printf("%s write successful: num_metrics=%d took=%s", bw.name, len(batch), time.Since(start))
		}()
	}

	timer := time.NewTimer(bw.interval)

	metrics := make([]influxdb.Metric, 0, 1e3)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			write(metrics)
			metrics = metrics[:0]
			timer.Reset(bw.interval)
		case m := <-bw.metricsStream:
			metrics = append(metrics, m)
			if len(metrics) < bw.maxSize {
				continue
			}
			write(metrics)
			metrics = metrics[:0]
			timer.Reset(bw.interval)
		}
	}
}

func (w *InfluxClient) BatchWriter(ctx context.Context, name string, maxSize, numWriters int, flushInterval time.Duration) *BatchWriter {
	return NewBatchWriter(ctx, name, w, maxSize, numWriters, flushInterval)
}

type FindOpts struct {
	Exchange  string
	Pair      Pair
	Start     time.Time
	End       time.Time
	Threshold time.Duration
	Interval  time.Duration
}

type Window struct {
	Start, End time.Time
}

func (w Window) String() string {
	startDur := time.Since(w.Start).Round(time.Minute)
	endDur := time.Since(w.End).Round(time.Minute)
	return fmt.Sprintf("start=%s end=%s", startDur, endDur)
}

const day = 24 * time.Hour

func (w *InfluxClient) FindWindow(ctx context.Context, opt FindOpts) ([]Window, error) {
	if err := w.readLimiter.Wait(ctx); err != nil {
		return nil, err
	}

	if opt.Threshold == 0 {
		opt.Threshold = 5 * time.Hour
	}
	if opt.Interval == 0 {
		opt.Interval = time.Minute
	}

	st := opt.Start
	end := opt.End

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
	|> keep(columns: ["_time"])
	|> unique(column: "_time")
	|> sort(columns: ["_time"], desc: true)
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
		return nil, err
	}
	defer res.Close()

	var windows []Window
	curWindow := Window{Start: time.Now()}
	for res.Next() {
		if len(res.Row) < 4 {
			continue
		}

		cur, err := time.Parse(time.RFC3339, res.Row[3])
		if err != nil {
			continue
		}

		if !curWindow.End.IsZero() && timeDiff(cur, curWindow.End) > opt.Threshold {
			windows = append(windows, curWindow)
			curWindow = Window{
				Start: cur,
				End:   cur,
			}
			continue
		}

		if cur.Before(curWindow.Start) {
			curWindow.Start = cur
		}
		if cur.After(curWindow.End) {
			curWindow.End = cur
		}
	}

	if len(windows) == 0 {
		if !curWindow.End.IsZero() {
			return []Window{validWindow(curWindow, opt.Interval)}, nil
		}
		return emptyRecordWindows(st, end, opt.Interval), nil
	}
	if curWindow != windows[len(windows)-1] {
		windows = append(windows, validWindow(curWindow, opt.Interval))
	}

	return windows, nil
}

func validWindow(w Window, interval time.Duration) Window {
	if w.Start.Equal(w.End) || timeDiff(w.Start, w.End) < interval {
		return Window{
			Start: w.End.Add(-interval),
			End:   w.End,
		}
	}
	return w
}

func emptyRecordWindows(start, end time.Time, interval time.Duration) []Window {
	//if timeDiff(start, end) <= interval {
	//	return []Window{{Start: start.Add(-interval), End: end}}
	//}

	var windows []Window
	for end.After(start) {
		st := end.Add(-interval)
		windows = append(windows, Window{
			Start: st,
			End:   end,
		})
		end = st
	}
	return windows
}

func timeDiff(start, end time.Time) time.Duration {
	return time.Duration(end.UnixNano() - start.UnixNano())
}
