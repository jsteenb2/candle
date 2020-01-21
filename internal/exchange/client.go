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
		FindWindow(ctx context.Context, opt FindOpts) ([]Window, error)
	}
)

type Runner struct {
	InfluxC   InfluxC
	Repo      Repository
	Exchanges []Exchange
}

type Metrics struct {
	Start, End time.Time
	Exchange   string
	Metrics    []influxdb.Metric
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
		case streams <- r.runHistorical(ctx, ex, pair, start):
		}
	}
	return streams, nil
}

func (r *Runner) runHistorical(ctx context.Context, ex Exchange, p Pair, startTime time.Time) <-chan Metrics {
	outStream := make(chan Metrics)
	go func(ctx context.Context, ex Exchange, p Pair, start time.Time) (e error) {
		defer func() {
			if e != nil && !errors.Is(e, context.Canceled) {
				log.Printf("%s (%s|%s) backfill err: %v", ex.Exchange(), p.Market(), p.Currency(), e)
			}
		}()
		defer close(outStream)

		track := struct {
			next time.Time
		}{
			next: time.Now(),
		}
		for {
			if track.next.Before(start) || track.next.Equal(start) {
				return
			}

			st := track.next.Add(-day)
			for _, w := range emptyRecordWindows(st, track.next, 4*time.Hour+30*time.Minute) {
				if err := r.history(ctx, outStream, ex, p, w.Start, w.End); err != nil {
					return err
				}
			}

			track.next = st
		}
	}(ctx, ex, p, startTime)
	return outStream
}

func (r *Runner) history(ctx context.Context, outStream chan Metrics, ex Exchange, p Pair, start, end time.Time) error {
	entries, err := ex.Historical(ctx, p, start.Add(-time.Minute), end)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	metrics := make([]influxdb.Metric, len(entries))
	var (
		st     = time.Now()
		endRes time.Time
	)
	for i, e := range entries {
		t := time.Unix(0, e.Time)
		if st.After(t) {
			st = t
		}
		if endRes.Before(t) {
			endRes = t
		}
		metrics[i] = influxdb.NewRowMetric(e.Fields(), p.Market(), map[string]string{
			"exchange": ex.Exchange(),
			"cur":      p.Currency(),
		}, t)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case outStream <- Metrics{
		End:      endRes,
		Start:    st,
		Exchange: ex.Exchange(),
		Metrics:  metrics,
	}:
		return nil
	}
}
