package config

import (
	"os"
	"testing"
)

func TestLoadYieldConfiguration(t *testing.T) {
	t.Setenv("JUSTLEND_YIELD_ENABLED", "true")
	t.Setenv("JUSTLEND_BASE_URL", "https://justlend.test")
	t.Setenv("TRON_STAKING_YIELD_ENABLED", "true")
	t.Setenv("TRON_HTTP_URL", "https://tron.test")
	t.Setenv("SOL_YIELD_ENABLED", "true")
	t.Setenv("SOLANA_RPC_URL", "https://solana.test")
	t.Setenv("SOL_VALIDATOR_VOTE_ACCOUNTS", "CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1")
	t.Setenv("JITO_SOL_BASE_URL", "https://jito.test")
	t.Setenv("MARINADE_APY_BASE_URL", "https://apy.test")
	t.Setenv("MARINADE_VALIDATORS_BASE_URL", "https://validators.test")
	t.Setenv("KAMINO_BASE_URL", "https://kamino.test")
	t.Setenv("SAVE_BASE_URL", "https://save.test")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.JustLendYieldEnabled || cfg.JustLendBaseURL != "https://justlend.test" || !cfg.TRONStakingYieldEnabled || cfg.TRONHTTPURL != "https://tron.test" || !cfg.SOLYieldEnabled || cfg.SolanaRPCURL != "https://solana.test" || len(cfg.SOLValidatorVoteAccounts) != 1 || cfg.JitoSOLBaseURL != "https://jito.test" || cfg.MarinadeAPYBaseURL != "https://apy.test" || cfg.MarinadeValidatorsBaseURL != "https://validators.test" || cfg.KaminoBaseURL != "https://kamino.test" || cfg.SaveBaseURL != "https://save.test" {
		t.Fatalf("yield config=%+v", cfg)
	}
}

func TestLoadRejectsInvalidOrDuplicateSOLVoteAccounts(t *testing.T) {
	t.Setenv("SOL_VALIDATOR_VOTE_ACCOUNTS", "bad")
	if _, err := Load(); err == nil {
		t.Fatal("invalid Solana vote account accepted")
	}
	t.Setenv("SOL_VALIDATOR_VOTE_ACCOUNTS", "CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1,CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1")
	if _, err := Load(); err == nil {
		t.Fatal("duplicate Solana vote account accepted")
	}
}

func TestLoadRejectsInvalidYieldBoolean(t *testing.T) {
	t.Setenv("JUSTLEND_YIELD_ENABLED", "sometimes")
	if _, err := Load(); err == nil {
		t.Fatal("invalid yield boolean accepted")
	}
}

func TestAVAXYieldConfigurationIsOptIn(t *testing.T) {
	t.Setenv("AVAX_YIELD_ENABLED", "")
	if err := os.Unsetenv("AVAX_YIELD_ENABLED"); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil || cfg.AVAXYieldEnabled {
		t.Fatalf("AVAX must default disabled: enabled=%t err=%v", cfg.AVAXYieldEnabled, err)
	}
	t.Setenv("AVAX_YIELD_ENABLED", "true")
	cfg, err = Load()
	if err != nil || !cfg.AVAXYieldEnabled {
		t.Fatalf("AVAX opt-in failed: enabled=%t err=%v", cfg.AVAXYieldEnabled, err)
	}
	t.Setenv("AVAX_YIELD_ENABLED", "sometimes")
	if _, err = Load(); err == nil {
		t.Fatal("invalid AVAX boolean accepted")
	}
}
