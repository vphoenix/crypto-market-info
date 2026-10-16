package across

import (
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (o *runtimeOwner) hotBytes() int64 {
	m := o.store.Metrics()
	n := runtimecheckpoint.EstimateTypedBytes(o.state) + runtimecheckpoint.EstimateTypedBytes(o.state.receiptParents) + m.HotBytes + m.VerifiedMetadataBytes
	for _, p := range []*runtimeCapturePages{o.state.activePages, o.state.partialPages} {
		if p != nil {
			n += p.records.MemoryBytes() + p.order.MemoryBytes()
		}
	}
	if o.state.receiptPages != nil {
		n += o.state.receiptPages.MemoryBytes() + o.state.receiptSchedule.MemoryBytes()
	}
	if o.state.retryPages != nil {
		n += o.state.retryPages.MemoryBytes() + o.state.skippedPages.MemoryBytes()
	}
	return n
}
func (o *runtimeOwner) capacity() error {
	if n := o.hotBytes(); n > 16<<20 {
		return fmt.Errorf("across runtime estimated hot bytes=%d: %w", n, runtimecheckpoint.ErrCapacity)
	}
	return nil
}
