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

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/pkg/httpc"
)

type Client struct {
	httpClient *httpc.Client
	wsURL      string
	symbols    map[string]bool
}

var _ exchange.Exchange = (*Client)(nil)

func New() (*Client, error) {
	client, err := httpc.New(httpc.WithAddr("https://api.gemini.com"))
	if err != nil {
		log.Panic(err)
	}

	c := &Client{
		httpClient: client,
		wsURL:      "wss://api.gemini.com/v2/marketdata",
	}
	if err := c.initSymbols(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) Exchange() string {
	return "gemini"
}

func (c *Client) ValidPair(pair exchange.Pair) bool {
	if exchange.Currency(pair.Currency()) == exchange.EUR {
		return false
	}
	if exchange.Market(pair.Market()) == exchange.Bitcoin {
		return true
	}

	pairSymb := strings.ToLower(pair.Market() + pair.Currency())
	return c.symbols[pairSymb]
}

// Subscribe grabs live data from the exchange.
//	docs: https://docs.gemini.com/websocket-api/#market-data
func (c *Client) Subscribe(ctx context.Context, pairs ...exchange.Pair) (<-chan exchange.PairEntryMsg, error) {
	conn, _, _, err := ws.Dial(ctx, c.wsURL)
	if err != nil {
		return nil, err
	}

	out := make(chan exchange.PairEntryMsg)
	go func(symbols []string) {
		defer close(out)
		defer conn.Close()

		type foo struct {
			Name    string   `json:"name"`
			Symbols []string `json:"symbols"`
		}

		subscription := struct {
			Type         string `json:"type"`
			Subscription []foo  `json:"subscriptions"`
		}{
			Type: "subscribe",
			Subscription: []foo{
				{
					Name:    "candles_1m",
					Symbols: symbols,
				},
			},
		}
		b, err := json.Marshal(subscription)
		if err != nil {
			log.Println(err)
			return
		}

		if err := wsutil.WriteClientText(conn, b); err != nil {
			log.Println(err)
			return
		}

		for {
			b, _, err := wsutil.ReadServerData(conn)
			if err != nil {
				log.Println(err)
				return
			}

			var resp struct {
				Type    string   `json:"type"`
				Symbol  string   `json:"symbol"`
				Changes []*entry `json:"changes"`
			}
			if err := json.Unmarshal(b, &resp); err != nil {
				log.Println(err)
				continue
			}

			if resp.Type == "heartbeat" {
				continue
			}

			pair := symToPair(resp.Symbol)
			for _, e := range resp.Changes {
				select {
				case <-ctx.Done():
					return
				case out <- exchange.PairEntryMsg{Pair: pair, Entry: exchange.Entry(*e)}:
				}
			}
		}
	}(pairsToSymbols(c.symbols, pairs))

	return out, nil
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

func (c *Client) initSymbols() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var symbols []string
	err := c.httpClient.
		Get("/v1/symbols").
		DecodeJSON(&symbols).
		Do(ctx)
	if err != nil {
		return err
	}

	m := make(map[string]bool, len(symbols))
	for _, s := range symbols {
		m[s] = true
	}
	c.symbols = m

	return nil
}

func candleID(p exchange.Pair) string {
	market := p.Market()
	switch exchange.Market(p.Market()) {
	case exchange.Bitcoin:
		market = "BTC"
	}

	return strings.ToLower(market + p.Currency())
}

func pairsToSymbols(available map[string]bool, pairs []exchange.Pair) []string {
	symbols := make([]string, 0, len(pairs))
	for _, p := range pairs {
		sym := candleID(p)
		if !available[sym] {
			continue
		}
		symbols = append(symbols, sym)
	}
	return symbols
}

func symToPair(symbol string) exchange.Pair {
	return exchange.NewPair(symbol[:3], symbol[3:])
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
