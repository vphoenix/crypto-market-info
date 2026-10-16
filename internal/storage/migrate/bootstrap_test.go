package migrate

import "testing"

func TestBootstrapClosureCoversRecoveryDomains(t *testing.T) {
	for family, count := range map[string]int{"crypto_market_info": 29, "crypto_market_info_across": 8, "crypto_market_info_reserve": 7, "crypto_market_info_lst": 8, "crypto_market_info_justlend_keeper": 7} {
		names, e := BootstrapTables(family)
		if e != nil || len(names) != count {
			t.Fatal(family, names, e)
		}
	}
	if _, e := BootstrapTables("unrelated_project"); e == nil {
		t.Fatal("unreviewed source family accepted")
	}
}
