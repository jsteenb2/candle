package gemini_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/jsteenb2/candle/internal/exchange"
	"github.com/jsteenb2/candle/internal/exchange/gemini"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	c, err := gemini.New()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	msgStream, err := c.Subscribe(ctx, exchange.XBTUSD)
	require.NoError(t, err)

	num := 0
	for msg := range msgStream {
		num++
		log.Println(msg)
	}
	t.Log("total: ", num)
}
