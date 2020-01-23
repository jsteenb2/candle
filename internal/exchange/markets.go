package exchange

import (
	"errors"
	"strings"
)

type (
	Market   string
	Currency string
)

const (
	MarketUnknown   Market = ""
	Bitcoin         Market = "BTC"
	BitcoinCash     Market = "BCH"
	Ethereum        Market = "ETH"
	EthereumClassic Market = "ETC"
	Monero          Market = "XMR"
	Ripple          Market = "XRP"
)

const (
	CurrencyUnknown Currency = ""
	USD             Currency = "USD"
	EUR             Currency = "EUR"
	USDT            Currency = "USDT"
)

var Pairs = func() []Pair {
	markets := []Market{Bitcoin, BitcoinCash, Ethereum, EthereumClassic, Monero, Ripple}
	currencies := []Currency{USD, EUR, USDT}

	var out []Pair
	for _, m := range markets {
		for _, c := range currencies {
			out = append(out, Pair{market: m, currency: c})
		}
	}
	return out
}

type Pair struct {
	market   Market
	currency Currency
}

func NewPair(market, cur string) Pair {
	return Pair{
		market:   Market(market),
		currency: Currency(cur),
	}
}

func (p Pair) IsCurrency(c Currency, rest ...Currency) bool {
	for _, cur := range append(rest, c) {
		if cur == p.currency {
			return true
		}
	}
	return false
}

func (p Pair) Market() string {
	return strings.ToUpper(string(p.market))
}

func (p Pair) Currency() string {
	return strings.ToUpper(string(p.currency))
}

func (p Pair) OK() error {
	var errs []string
	if p.market == "" {
		errs = append(errs, "market is not set")
	}
	if p.currency == "" {
		errs = append(errs, "currency is not set")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, " and "))
	}
	return nil
}

var (
	BTCUSD = Pair{
		market:   Bitcoin,
		currency: USD,
	}

	BTCEUR = Pair{
		market:   Bitcoin,
		currency: EUR,
	}

	BCHUSD = Pair{
		market:   BitcoinCash,
		currency: USD,
	}

	BCHEUR = Pair{
		market:   BitcoinCash,
		currency: EUR,
	}

	ETHUSD = Pair{
		market:   Ethereum,
		currency: USD,
	}

	ETHEUR = Pair{
		market:   Ethereum,
		currency: EUR,
	}

	ETCUSD = Pair{
		market:   EthereumClassic,
		currency: USD,
	}

	ETCEUR = Pair{
		market:   EthereumClassic,
		currency: EUR,
	}

	XMRUSD = Pair{
		market:   Monero,
		currency: USD,
	}

	XMREUR = Pair{
		market:   Monero,
		currency: EUR,
	}

	XRPUSD = Pair{
		market:   Ripple,
		currency: USD,
	}

	XRPEUR = Pair{
		market:   Ripple,
		currency: EUR,
	}
)
