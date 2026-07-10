package source_test

import (
	"context"
	"testing"

	"github.com/adinvadim/exchangecrawl/internal/model"
	"github.com/adinvadim/exchangecrawl/internal/source"
)

func TestAdapterContractAcceptsOpaqueCursor(t *testing.T) {
	t.Parallel()

	var adapter source.Adapter = contractAdapter{}
	page, err := adapter.FetchPage(context.Background(), source.PageRequest{Cursor: "opaque:value"})
	if err != nil {
		t.Fatalf("FetchPage() error = %v", err)
	}
	if page.NextCursor != "opaque:value:next" {
		t.Fatalf("NextCursor = %q", page.NextCursor)
	}
}

type contractAdapter struct{}

func (contractAdapter) Exchange() model.Exchange { return model.ExchangeBybit }

func (contractAdapter) CheckCredentials(source.Account) source.CredentialStatus {
	return source.CredentialStatus{Ready: true}
}

func (contractAdapter) FetchPage(_ context.Context, request source.PageRequest) (source.Page, error) {
	return source.Page{NextCursor: request.Cursor + ":next"}, nil
}
