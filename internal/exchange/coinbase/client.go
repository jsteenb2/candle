package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/pkg/httpc"
	"golang.org/x/time/rate"
)

const (
	iso8601Fmt = "2006-01-02T15:04:05-0700"
)

type Client struct {
	httpClient *httpc.Client
	limiter    *rate.Limiter
}

var _ exchange.Exchange = (*Client)(nil)

func New() (*Client, error) {
	client, err := httpc.New(httpc.WithAddr("https://api.pro.coinbase.com"))
	if err != nil {
		log.Panic(err)
	}

	limiter := rate.NewLimiter(3, 3)
	limiter.SetLimit(rate.Every(time.Second))

	return &Client{
		httpClient: client,
		limiter:    limiter,
	}, nil
}

func (c *Client) Exchange() string {
	return "coinbase"
}

func (c *Client) ValidPair(pair exchange.Pair) bool {
	return exchange.Market(pair.Market()) != exchange.Monero
}

func (c *Client) Historical(ctx context.Context, pair exchange.Pair, start, end time.Time) ([]exchange.Entry, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	f, err := c.getHistorical(ctx, pair, start, end)
	if err != nil {
		return nil, err
	}

	out := make([]exchange.Entry, len(f))
	for i, e := range f {
		out[i] = exchange.Entry(*e)
	}

	return out, nil
}

func (c *Client) getHistorical(ctx context.Context, pair exchange.Pair, start, end time.Time) ([]*entry, error) {
	var (
		f   []*entry
		err error
	)

	tsStart := timestamp(start)
	if end.UnixNano()-start.UnixNano() > 5*int64(time.Hour) || end.IsZero() {
		end = start.Add(5*time.Hour - time.Minute)
	}

	tsEnd := timestamp(end)

	for i := 0; i < 10; i++ {
		params := [][2]string{{"granularity", "60"}}
		if !start.IsZero() && !end.IsZero() {
			params = append(params,
				[2]string{"start", tsStart},
				[2]string{"end", tsEnd},
			)
		}

		var retry bool
		err = c.httpClient.
			Get("/products", productID(pair), "candles").
			QueryParams(params...).
			DecodeJSON(&f).
			StatusFn(func(resp *http.Response) error {
				if resp.StatusCode == 200 {
					return nil
				}
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				retry = resp.StatusCode == 429
				return fmt.Errorf("status_code=%d market=%s currency=%s err=%s", resp.StatusCode, pair.Market(), pair.Currency(), string(b))
			}).
			Do(ctx)
		if err == nil {
			break
		}
		if !retry {
			break
		}
		log.Printf("%s retryable err: %v", c.Exchange(), err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return f, err
}

func productID(p exchange.Pair) string {
	market := p.Market()
	switch exchange.Market(p.Market()) {
	case exchange.Bitcoin:
		market = "BTC"
	}

	return market + "-" + p.Currency()
}

func timestamp(t time.Time) string {
	return t.Format(iso8601Fmt)
}

type entry exchange.Entry

func (e *entry) UnmarshalJSON(b []byte) error {
	var f [12]interface{}
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}

	e.Time = int64(intIface(f[0])) * int64(time.Second)
	e.Low = floatIface(f[1])
	e.High = floatIface(f[2])
	e.Open = floatIface(f[3])
	e.Close = floatIface(f[4])
	e.Volume = floatIface(f[5])

	return nil
}

func intIface(v interface{}) int {
	i, ok := v.(int)
	if ok {
		return i
	}
	f, _ := v.(float64)
	return int(f)
}

func floatIface(v interface{}) float64 {
	o, ok := v.(string)
	if !ok {
		f, _ := v.(float64)
		return f
	}
	f, _ := strconv.ParseFloat(o, 64)
	return f
}
