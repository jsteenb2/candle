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
	Bitcoin         Market = "XBT"
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
)

type Pair struct {
	market   Market
	currency Currency
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
	XBTUSD = Pair{
		market:   Bitcoin,
		currency: USD,
	}

	XBTEUR = Pair{
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
