package optionslive

import "testing"

func TestCatalogScopesFollowKinds(t *testing.T) {
	if n := len(Config{}.catalogScopes()); n != 6 {
		t.Fatalf("default scopes = %d, want 6", n)
	}
	ss := Config{Kinds: []string{"future"}}.catalogScopes()
	if len(ss) != 3 {
		t.Fatalf("future scopes = %d, want 3", len(ss))
	}
	for _, s := range ss {
		if s.Kind != "future" {
			t.Fatalf("unexpected scope %s", s)
		}
	}
}

func TestKindsValidation(t *testing.T) {
	base := Config{RESTURL: "https://www.deribit.com", WSURL: "wss://www.deribit.com/ws/api/v2"}
	for _, kinds := range [][]string{{"future"}, {"option"}, {"option", "future"}} {
		c := base
		c.Kinds = kinds
		if err := c.Validate(); err != nil {
			t.Fatalf("%v: %v", kinds, err)
		}
	}
	for _, kinds := range [][]string{{"perpetual"}, {"future", "future"}, {""}} {
		c := base
		c.Kinds = kinds
		if c.Validate() == nil {
			t.Fatalf("%v accepted", kinds)
		}
	}
	c := base
	c.Kinds = []string{"future"}
	c.Symbols = []string{"BTC-26MAR27-100000-C"}
	if c.Validate() == nil {
		t.Fatal("option symbol accepted in futures-only mode")
	}
	c.Symbols = []string{"BTC-26MAR27"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
