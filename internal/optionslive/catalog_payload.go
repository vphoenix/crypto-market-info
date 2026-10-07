package optionslive

import (
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/options"
)

// Typed catalog and lifecycle observations retain the source hash and all
// collection/replay inputs. Validate the received bytes before writing those
// observations; neither collection nor replay needs a second copy on disk.
func validateCatalogPayload(raw []byte, hash string) error {
	if len(raw) == 0 {
		if hash != "" {
			return fmt.Errorf("evidence hash without payload")
		}
		return nil
	}
	if options.PayloadHash(raw) != hash {
		return fmt.Errorf("payload hash mismatch")
	}
	return nil
}
