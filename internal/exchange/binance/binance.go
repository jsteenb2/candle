package binance

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

type Client struct {
	httpClient *httpc.Client
	limiter    *rate.Limiter

	knownSymbols map[string]bool
}

func New() (*Client, error) {
	client, err := httpc.New(httpc.WithAddr("https://api.binance.com"))
	if err != nil {
		log.Panic(err)
	}

	limit := rate.Every(500 * time.Millisecond)
	limiter := rate.NewLimiter(5, 5)
	limiter.SetLimit(limit)

	c := &Client{
		httpClient: client,
		limiter:    limiter,
	}
	if err := c.initSymbols(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) Exchange() string {
	return "binance"
}

func (c *Client) ValidPair(pair exchange.Pair) bool {
	return c.knownSymbols[symbol(pair)]
}

func (c *Client) Subscribe(ctx context.Context, pairs ...exchange.Pair) (<-chan exchange.PairEntryMsg, error) {
	stream := make(chan exchange.PairEntryMsg)
	defer close(stream)

	return stream, nil
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

	tsStart, tsEnd := timestamp(start), timestamp(end)
	for i := 0; i < 10; i++ {
		params := [][2]string{
			{"symbol", symbol(pair)},
			{"interval", "1m"},
			{"limit", "1000"},
		}
		if !start.IsZero() {
			params = append(params,
				[2]string{"startTime", tsStart},
				[2]string{"endTime", tsEnd},
			)
		}

		var retry bool
		err := c.httpClient.
			Get("/api/v3/klines").
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

func (c *Client) initSymbols() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var body struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
		} `json:"symbols"`
	}
	err := c.httpClient.
		Get("/api/v3/exchangeInfo").
		DecodeJSON(&body).
		Do(ctx)
	if err != nil {
		return err
	}

	m := make(map[string]bool)
	for _, s := range body.Symbols {
		m[s.Symbol] = true
	}
	c.knownSymbols = m

	return nil
}

func symbol(p exchange.Pair) string {
	market := p.Market()
	if exchange.Market(p.Market()) == exchange.Bitcoin {
		market = "BTC"
	}

	cur := p.Currency()
	if exchange.Currency(cur) == exchange.USD {
		cur = "USDT"
	}
	return market + cur
}

func timestamp(t time.Time) string {
	return strconv.FormatInt(t.UnixNano()/int64(time.Millisecond), 10)
}

type entry exchange.Entry

func (e *entry) UnmarshalJSON(b []byte) error {
	var f [12]interface{}
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}

	e.Time = int64(intIface(f[0])) * int64(time.Millisecond)
	e.Open = floatIface(f[1])
	e.High = floatIface(f[2])
	e.Low = floatIface(f[3])
	e.Close = floatIface(f[4])
	e.Volume = floatIface(f[5])
	e.Count = intIface(f[8])

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
