package tx

import (
	"context"
	"testing"
)

func TestWithTxRoundTrip(t *testing.T) {
	t.Parallel()

	handle := &Handle{id: "promo-1", state: TxActive}
	ctx := WithTx(context.Background(), handle)

	got, ok := GetTx(ctx)
	if !ok {
		t.Fatal("expected transaction in context")
	}
	if got.ID() != "promo-1" {
		t.Fatalf("GetTx().ID() = %s, want promo-1", got.ID())
	}
}
