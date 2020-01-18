package kraken

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/pkg/httpc"
)

type Client struct {
	httpClient *httpc.Client
}

var _ exchange.Exchange = (*Client)(nil)

func New() (*Client, error) {
	client, err := httpc.New(httpc.WithAddr("https://api.kraken.com"))
	if err != nil {
		log.Panic(err)
	}

	return &Client{httpClient: client}, nil
}

func (c *Client) Exchange() string {
	return "kraken"
}

func (c *Client) ValidPair(_ exchange.Pair) bool {
	return true
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
