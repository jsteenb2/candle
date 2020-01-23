package kraken

import (
	"context"
	"encoding/json"
	"fmt"
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
}

var _ exchange.Exchange = (*Client)(nil)

func New() (*Client, error) {
	client, err := httpc.New(httpc.WithAddr("https://api.kraken.com"))
	if err != nil {
		log.Panic(err)
	}

	return &Client{
		httpClient: client,
		wsURL:      "wss://ws.kraken.com/v2/marketdata",
	}, nil
}

func (c *Client) Exchange() string {
	return "kraken"
}

func (c *Client) ValidPair(_ exchange.Pair) bool {
	return true
}

// Subscribe grabs live data from the exchange.
//	docs: https://docs.kraken.com/websockets/
func (c *Client) Subscribe(ctx context.Context, pairs ...exchange.Pair) (<-chan exchange.PairEntryMsg, error) {
	conn, _, _, err := ws.Dial(ctx, c.wsURL)
	if err != nil {
		return nil, err
	}

	out := make(chan exchange.PairEntryMsg)
	go func(symbols []string) {
		defer close(out)
		defer conn.Close()

		type sub struct {
			Name     string `json:"name"`
			Interval int    `json:"interval"` // in minutes
		}

		subscription := struct {
			Event        string   `json:"event"`
			Pairs        []string `json:"pair"`
			Subscription sub      `json:"subscription"`
		}{
			Event: "subscribe",
			Pairs: symbols,
			Subscription: sub{
				Name:     "ohlc",
				Interval: 1,
			},
		}
		b, err := json.Marshal(subscription)
		if err != nil {
			log.Println(err)
			return
		}

		if err := wsutil.WriteClientText(conn, b); err != nil {
			log.Println("write client text: ", err)
			return
		}

		tracker := make(map[string]struct {
			endTime time.Time
			msg     exchange.PairEntryMsg
		})
		for {
			b, _, err := wsutil.ReadServerData(conn)
			if err != nil {
				log.Println("read server data:", err)
				return
			}

			var resp interface{}
			if err := json.Unmarshal(b, &resp); err != nil {
				log.Println("json unmarshal: ", err)
				continue
			}

			switch p := resp.(type) {
			case map[string]interface{}:
				if event, ok := p["event"].(string); !ok || event == "heartbeat" {
					// do something?
				}
			case []interface{}:
				e, ok := slcIfaceToEntry(tracker, p)
				if !ok {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case out <- e:
				}
			default:
				log.Printf("unmatched type: %+v", p)
				continue
			}
		}
	}(pairsToSymbols(pairs))

	return out, nil
}

func (c *Client) Historical(ctx context.Context, pair exchange.Pair, start, _ time.Time) ([]exchange.Entry, error) {
	if time.Since(start) > 12*time.Hour {
		return nil, nil
	}

	deets := convertPair(pair)
	err := c.httpClient.
		Get("/0/public/OHLC").
		QueryParams(
			[2]string{"pair", pair.Market() + pair.Currency()},
			[2]string{"since", timestamp(start)},
		).
		Decode(deets.Decode).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return deets.Entries(), nil
}

func slcIfaceToEntry(cache map[string]struct {
	endTime time.Time
	msg     exchange.PairEntryMsg
}, slc []interface{}) (exchange.PairEntryMsg, bool) {
	if len(slc) < 4 {
		return exchange.PairEntryMsg{}, false
	}
	symbol, ok := slc[3].(string)
	if !ok {
		return exchange.PairEntryMsg{}, false
	}
	if !strings.Contains(symbol, "/") {
		return exchange.PairEntryMsg{}, false
	}

	entry, ok := slc[1].([]interface{})
	if !ok || len(entry) != 9 {
		return exchange.PairEntryMsg{}, false
	}

	existing, ok := cache[symbol]
	endTime := time.Unix(0, int64(floatIface(entry[1]))*int64(time.Second))

	ex := exchange.PairEntryMsg{
		Pair: exchange.NewPair(symbol[:3], symbol[4:]),
		Entry: exchange.Entry{
			Time:   int64((time.Duration(floatIface(entry[0])) * time.Second).Round(time.Minute)),
			Open:   floatIface(entry[2]),
			High:   floatIface(entry[3]),
			Low:    floatIface(entry[4]),
			Close:  floatIface(entry[5]),
			Vwap:   floatIface(entry[6]),
			Volume: floatIface(entry[7]),
			Count:  intIface(entry[8]),
		},
	}
	cache[symbol] = struct {
		endTime time.Time
		msg     exchange.PairEntryMsg
	}{
		endTime: endTime,
		msg:     ex,
	}
	return existing.msg, ok && existing.endTime.Before(endTime)
}

func pairsToSymbols(pairs []exchange.Pair) []string {
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.Market()+"/"+p.Currency())
	}
	return out
}

type resulter interface {
	Decode(resp *http.Response) error
	Entries() []exchange.Entry
	Since() int
}

func convertPair(p exchange.Pair) resulter {
	switch p {
	case exchange.XBTUSD:
		return new(xbtusdMark)
	case exchange.XBTEUR:
		return new(xbteurMark)
	case exchange.BCHUSD:
		return new(bchusdMark)
	case exchange.BCHEUR:
		return new(bcheurMark)
	case exchange.ETHUSD:
		return new(ethusdMark)
	case exchange.ETHEUR:
		return new(etheurMark)
	case exchange.ETCUSD:
		return new(etcusdMark)
	case exchange.ETCEUR:
		return new(etceurMark)
	case exchange.XMRUSD:
		return new(xmrusdMark)
	case exchange.XMREUR:
		return new(xmreurMark)
	case exchange.XRPUSD:
		return new(xrpusdMark)
	case exchange.XRPEUR:
		return new(xrpeurMark)
	default:
		return nil
	}
}

func decodeOHLC(v interface{}) func(resp *http.Response) error {
	return func(resp *http.Response) error {
		body := struct {
			Errs   []interface{} `json:"error"`
			Result interface{}   `json:"result"`
		}{
			Result: v,
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return err
		}
		if len(body.Errs) > 0 {
			return fmt.Errorf("kraken errs: %v", body.Errs)
		}
		return nil
	}
}

func timestamp(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}

type entry exchange.Entry

func (e entry) String() string {
	return fmt.Sprintf(
		"time=%d count=%d open=%0.2f close=%0.2f high=%0.2f low=%0.2f volume=%0.2f vwap=%0.2f",
		e.Time,
		e.Count,
		e.Open,
		e.Close,
		e.High,
		e.Low,
		e.Volume,
		e.Vwap,
	)
}

func (e *entry) UnmarshalJSON(b []byte) error {
	var decE [8]interface{}
	if err := json.Unmarshal(b, &decE); err != nil {
		return err
	}

	e.Time = int64(intIface(decE[0])) * int64(time.Second)
	e.Count = intIface(decE[7])
	e.Open = floatIface(decE[1])
	e.Close = floatIface(decE[4])
	e.High = floatIface(decE[2])
	e.Low = floatIface(decE[3])
	e.Vwap = floatIface(decE[5])
	e.Volume = floatIface(decE[6])

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
