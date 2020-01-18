package exchange

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/influxdata/influxdb-client-go"
)

type Entry struct {
	Time  int64
	Count int

	Open, Close float64
	High, Low   float64

	Volume, Vwap float64
}

func (e Entry) Fields() map[string]interface{} {
	fields := make(map[string]interface{})
	if e.Count > 0 {
		fields["count"] = e.Count
	}

	applyNonZeroFloat := func(key string, f float64) {
		if f > 0 {
			fields[key] = f
		}
	}
	applyNonZeroFloat("open", e.Open)
	applyNonZeroFloat("close", e.Close)
	applyNonZeroFloat("high", e.High)
	applyNonZeroFloat("low", e.Low)
	applyNonZeroFloat("volume", e.Volume)
	applyNonZeroFloat("vwap", e.Vwap)
	return fields
}

type (
	Repository interface {
		GetLast(key string) time.Time
		PutLast(key string, t time.Time) error
	}

	Exchange interface {
		Exchange() string
		Historical(ctx context.Context, pair Pair, start, end time.Time) ([]Entry, error)
		ValidPair(pair Pair) bool
	}

	InfluxC interface {
		FindWindow(ctx context.Context, opt FindOpts) (time.Time, time.Time, error)
	}
)

type Runner struct {
	InfluxC   InfluxC
	Repo      Repository
	Exchanges []Exchange
}

const queryFmt = `
from(bucket: "bucket1")
	|> range(start: -%dns, stop: -%dns)
	|> filter(fn : (r) => 
		r.exchange == %q and 
		r._measurement == %q and 
		r.cur == %q
	)
	|> aggregateWindow(every: 1m, fn: count)
	|> filter(fn: (r) => r._value == 0)  
	|> sort(columns: ["_time"], desc: true)
	|> keep(columns: ["_time"])
	|> limit(n: 500)
`

type Metrics struct {
	Exchange string
	Metrics  []influxdb.Metric
}

func (r *Runner) BackFill(ctx context.Context, pair Pair, start time.Time) (<-chan (<-chan Metrics), error) {
	streams := make(chan (<-chan Metrics), len(r.Exchanges))
	defer close(streams)

	for _, ex := range r.Exchanges {
		if !ex.ValidPair(pair) {
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case streams <- r.history(ctx, ex, pair, start):
		}
	}
	return streams, nil
}

func (r *Runner) history(ctx context.Context, ex Exchange, p Pair, start time.Time) <-chan Metrics {
	outStream := make(chan Metrics)

	go func(ctx context.Context, ex Exchange, p Pair, start time.Time) (e error) {
		defer func() {
			if e != nil && !errors.Is(e, context.Canceled) {
				log.Printf("%s (%s|%s) backfill err: %v", ex.Exchange(), p.Market(), p.Currency(), e)
			}
		}()
		defer close(outStream)

		track := struct {
			end     time.Time
			counter int
			st      time.Time
			next    time.Time
		}{
			next: time.Now(),
		}
		for i := 0; ; i++ {
			if track.next.Before(start) {
				return
			}

			st, end, err := r.InfluxC.FindWindow(ctx, FindOpts{
				Exchange: ex.Exchange(),
				Pair:     p,
				Start:    start.Add(-2 * time.Minute),
				End:      track.next,
			})
			if err != nil {
				return err
			}

			if st.IsZero() || track.end.Equal(end) && track.counter > 0 || track.st.Equal(st) {
				return nil
			}

			entries, err := ex.Historical(ctx, p, st.Add(-time.Minute), end)
			if err != nil {
				return err
			}

			track.counter++
			track.st = st
			track.end = end
			if st.Before(track.next) {
				track.next = st.Add(-time.Minute)
			} else if end.Before(track.next) {
				track.next = st.Add(-time.Minute)
			} else {
				track.next = track.next.Add(-5*time.Hour + 2*time.Minute)
			}

			if len(entries) == 0 {
				continue
			}

			metrics := make([]influxdb.Metric, len(entries))
			for i, e := range entries {
				metrics[i] = influxdb.NewRowMetric(e.Fields(), p.Market(), map[string]string{
					"exchange": ex.Exchange(),
					"cur":      p.Currency(),
				}, time.Unix(0, e.Time))
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outStream <- Metrics{Exchange: ex.Exchange(), Metrics: metrics}:
				track.counter = 0
			}
		}
	}(ctx, ex, p, start)
	return outStream
}
