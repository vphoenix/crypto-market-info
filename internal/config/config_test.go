package config

import "testing"

func TestLoadYieldConfiguration(t *testing.T) {
	t.Setenv("JUSTLEND_YIELD_ENABLED", "true")
	t.Setenv("JUSTLEND_BASE_URL", "https://justlend.test")
	t.Setenv("TRON_STAKING_YIELD_ENABLED", "true")
	t.Setenv("TRON_HTTP_URL", "https://tron.test")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.JustLendYieldEnabled || cfg.JustLendBaseURL != "https://justlend.test" || !cfg.TRONStakingYieldEnabled || cfg.TRONHTTPURL != "https://tron.test" {
		t.Fatalf("yield config=%+v", cfg)
	}
}

func TestLoadRejectsInvalidYieldBoolean(t *testing.T) {
	t.Setenv("JUSTLEND_YIELD_ENABLED", "sometimes")
	if _, err := Load(); err == nil {
		t.Fatal("invalid yield boolean accepted")
	}
}
