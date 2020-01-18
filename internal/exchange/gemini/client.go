package gemini

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/pkg/httpc"
)

type Client struct {
	httpClient *httpc.Client
}

var _ exchange.Exchange = (*Client)(nil)

func New() (*Client, error) {
	client, err := httpc.New(httpc.WithAddr("https://api.gemini.com"))
	if err != nil {
		log.Panic(err)
	}

	return &Client{httpClient: client}, nil
}

func (c *Client) Exchange() string {
	return "gemini"
}

func (c *Client) Interval(pair exchange.Pair) time.Duration {
	// interval is high b/c gemini gives us a boat load of data
	// and there is no way to paginate it
	if exchange.Currency(pair.Currency()) == exchange.USD {
		return 30 * time.Minute
	}
	return 20 * time.Minute
}

func (c *Client) ValidPair(pair exchange.Pair) bool {
	if exchange.Currency(pair.Currency()) == exchange.EUR {
		return false
	}
	if exchange.Market(pair.Market()) == exchange.Bitcoin {
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var symbols []string
	err := c.httpClient.
		Get("/v1/symbols").
		DecodeJSON(&symbols).
		Do(ctx)
	if err != nil {
		log.Println("failed to get gemini symbols: ", err)
		// fail to true
		return true
	}

	pairSymb := strings.ToLower(pair.Market() + pair.Currency())

	for _, s := range symbols {
		if s == pairSymb {
			return true
		}
	}
	return false
}

func (c *Client) Historical(ctx context.Context, pair exchange.Pair, start, end time.Time) ([]exchange.Entry, error) {
	if time.Since(start) > 12*time.Hour || end.IsZero() {
		return nil, nil
	}

	// pair is ignored here since gemini doesn't have pagination
	f, err := c.getHistorical(ctx, pair)
	if err != nil {
		return nil, err
	}
	out := make([]exchange.Entry, len(f))
	for i, e := range f {
		out[i] = exchange.Entry(*e)
	}
	return out, nil
}

func (c *Client) getHistorical(ctx context.Context, pair exchange.Pair) ([]*entry, error) {
	var (
		f   []*entry
		err error
	)
	for i := 0; i < 10; i++ {
		var retry bool
		err := c.httpClient.
			Get("/v2/candles/", candleID(pair), "/1m").
			DecodeJSON(&f).
			StatusFn(func(resp *http.Response) error {
				code := resp.StatusCode
				if code == 200 {
					return nil
				}
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				retry = code == 429 || code == 500
				return fmt.Errorf("status_code=%d market=%s currency=%s err=%s", resp.StatusCode, pair.Market(), pair.Currency(), string(b))
			}).
			Do(ctx)
		if err == nil {
			break
		}
		if !retry {
			break
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return f, err
}

func candleID(p exchange.Pair) string {
	market := p.Market()
	switch exchange.Market(p.Market()) {
	case exchange.Bitcoin:
		market = "BTC"
	}

	return strings.ToLower(market + p.Currency())
}

type entry exchange.Entry

func (e *entry) UnmarshalJSON(b []byte) error {
	var f [6]interface{}
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}

	e.Time = int64(intIface(f[0])) * int64(time.Millisecond)
	e.Open = floatIface(f[1])
	e.High = floatIface(f[2])
	e.Low = floatIface(f[3])
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
