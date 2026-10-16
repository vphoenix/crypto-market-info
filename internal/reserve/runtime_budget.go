package reserve

import (
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (o *runtimeOwner) hotBytes() int64 {
	m := o.files.Metrics()
	n := runtimecheckpoint.EstimateTypedBytes(o.state) + m.HotBytes + m.VerifiedMetadataBytes
	for _, p := range []*runtimecheckpoint.PagedMap[runtimeHeader]{o.state.active, o.state.final, o.state.pending} {
		if p != nil {
			n += p.MemoryBytes()
		}
	}
	return n
}
func (o *runtimeOwner) capacity() error {
	if n := o.hotBytes(); n > 16<<20 {
		return fmt.Errorf("reserve runtime estimated hot bytes=%d: %w", n, runtimecheckpoint.ErrCapacity)
	}
	return nil
}
