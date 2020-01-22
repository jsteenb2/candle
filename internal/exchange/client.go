package exchange

import (
	"context"
	"errors"
	"log"
	"sync"
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

func (e Entry) toInfluxMetric(exchange string, p Pair) influxdb.Metric {
	return influxdb.NewRowMetric(e.Fields(), p.Market(), map[string]string{
		"exchange": exchange,
		"cur":      p.Currency(),
	}, time.Unix(0, e.Time))
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

	PairEntryMsg struct {
		Pair  Pair
		Entry Entry
	}

	Exchange interface {
		Exchange() string
		Historical(ctx context.Context, pair Pair, start, end time.Time) ([]Entry, error)
		Subscribe(ctx context.Context, pairs ...Pair) (<-chan PairEntryMsg, error)
		ValidPair(pair Pair) bool
	}

	InfluxC interface {
		FindWindow(ctx context.Context, opt FindOpts) ([]Window, error)
		Write(ctx context.Context, metrics []influxdb.Metric) error
	}
)

type Runner struct {
	InfluxC   InfluxC
	Repo      Repository
	Exchanges []Exchange
}

type (
	Metrics struct {
		Pair       Pair
		Start, End time.Time
		Exchange   string
		Metrics    []influxdb.Metric
	}

	Metric struct {
		Exchange string
		Pair     Pair
		Metric   influxdb.Metric
	}
)

func (r *Runner) Live(ctx context.Context, pairs []Pair) error {
	metricStream := make(chan Metric, len(r.Exchanges))

	wg := new(sync.WaitGroup)
	for _, ex := range r.Exchanges {
		stream, err := ex.Subscribe(ctx, pairs...)
		if err != nil {
			log.Printf("%s subscribe err: %s", ex.Exchange(), err)
			continue
		}
		wg.Add(1)
		go func(exchange string, st <-chan PairEntryMsg) {
			defer wg.Done()
			for m := range st {
				metricStream <- Metric{
					Pair:     m.Pair,
					Exchange: exchange,
					Metric:   m.Entry.toInfluxMetric(exchange, m.Pair),
				}
			}
		}(ex.Exchange(), stream)
	}
	go func() {
		wg.Wait()
		close(metricStream)
	}()

	for m := range metricStream {
		metrics := []influxdb.Metric{m.Metric}
		if err := r.InfluxC.Write(ctx, metrics); err != nil {
			log.Printf("failed live write to influxdb for market=%s cur=%s: err=%s", m.Pair.Market(), m.Pair.Currency(), err)
			continue
		}
		log.Printf("live write successful: exchange=%s market=%s cur=%s time=%s", m.Exchange, m.Pair.Market(), m.Pair.Currency(), m.Metric.Time().Format(time.Stamp))
	}

	return nil
}

func (r *Runner) BackFill(ctx context.Context, pairs []Pair, start time.Time) error {
	metricsStream := make(chan Metrics)

	wg := new(sync.WaitGroup)
	for _, ex := range r.Exchanges {
		for _, pair := range pairs {
			if !ex.ValidPair(pair) {
				continue
			}

			stream := r.runHistorical(ctx, ex, pair, start)

			wg.Add(1)
			go func(st <-chan Metrics) {
				defer wg.Done()

				for msg := range st {
					metricsStream <- msg
				}
			}(stream)
		}
	}

	go func() {
		wg.Wait()
		close(metricsStream)
	}()

	for m := range metricsStream {
		if err := r.InfluxC.Write(context.Background(), m.Metrics); err != nil {
			log.Printf("failed backfill write to influxdb for market=%s cur=%s: err=%s", m.Pair.Market(), m.Pair.Currency(), err)
			continue
		}
		log.Printf("backfill successful: exchange=%s market=%s cur=%s batch_size=%d start=%q stop=%q spans=%s",
			m.Exchange,
			m.Pair.Market(),
			m.Pair.Currency(),
			len(m.Metrics),
			m.Start.Format(time.Stamp),
			m.End.Format(time.Stamp),
			time.Duration(m.End.UnixNano()-m.Start.UnixNano()),
		)
	}

	return nil
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
		Pair:     p,
		End:      endRes,
		Start:    st,
		Exchange: ex.Exchange(),
		Metrics:  metrics,
	}:
		return nil
	}
}
