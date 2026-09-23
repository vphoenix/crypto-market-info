package config

import "testing"

func TestOptionsConfiguration(t *testing.T) {
	t.Setenv("OPTIONS_ENABLED", "true")
	t.Setenv("OPTIONS_SYMBOLS", "BTC-23SEP26-81000-C,BTC-23SEP26-81000-P,BTC-23SEP26")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.Options.Enabled || len(c.Options.Symbols) != 3 {
		t.Fatal("options config not loaded")
	}
	t.Setenv("OPTIONS_SYMBOLS", "auto")
	c, err = Load()
	if err != nil || len(c.Options.Symbols) != 0 {
		t.Fatal("auto config rejected")
	}
	t.Setenv("OPTIONS_SYMBOLS", "BTC-23SEP26,BTC-23SEP26")
	if _, err = Load(); err == nil {
		t.Fatal("duplicate accepted")
	}
	t.Setenv("OPTIONS_SYMBOLS", "auto")
	t.Setenv("DERIBIT_REST_URL", "https://user:secret@example.invalid")
	if _, err = Load(); err == nil {
		t.Fatal("credential URL accepted")
	}
}
