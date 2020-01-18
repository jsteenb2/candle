package kraken

import (
	"net/http"

	"github.com/jsteenb2/candle/internal/exchange"
)

type entries []*entry

func (e entries) ExEntries() []exchange.Entry {
	out := make([]exchange.Entry, len(e))
	for i, ent := range e {
		out[i] = exchange.Entry(*ent)
	}
	return out
}

type xbtusdMark struct {
	En entries `json:"XXBTZUSD"`
	last
}

func (b *xbtusdMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *xbtusdMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type xbteurMark struct {
	En entries `json:"XXBTZEUR"`
	last
}

func (b *xbteurMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *xbteurMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type bchusdMark struct {
	En entries `json:"BCHUSD"`
	last
}

func (b *bchusdMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *bchusdMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type bcheurMark struct {
	En entries `json:"BCHEUR"`
	last
}

func (b *bcheurMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *bcheurMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type ethusdMark struct {
	En entries `json:"XETHZUSD"`
	last
}

func (b *ethusdMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *ethusdMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type etheurMark struct {
	En entries `json:"XETHZEUR"`
	last
}

func (b *etheurMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *etheurMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type etcusdMark struct {
	En entries `json:"XETCZUSD"`
	last
}

func (b *etcusdMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *etcusdMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type etceurMark struct {
	En entries `json:"XETCZEUR"`
	last
}

func (b *etceurMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *etceurMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type xmrusdMark struct {
	En entries `json:"XXMRZUSD"`
	last
}

func (b *xmrusdMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *xmrusdMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type xmreurMark struct {
	En entries `json:"XXMRZEUR"`
	last
}

func (b *xmreurMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *xmreurMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type xrpusdMark struct {
	En entries `json:"XXRPZUSD"`
	last
}

func (b *xrpusdMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *xrpusdMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type xrpeurMark struct {
	En entries `json:"XXRPZEUR"`
	last
}

func (b *xrpeurMark) Decode(resp *http.Response) error {
	return decodeOHLC(b)(resp)
}

func (b *xrpeurMark) Entries() []exchange.Entry {
	return b.En.ExEntries()
}

type last struct {
	Last int `json:"last"`
}

func (l last) Since() int {
	return l.Last
}
