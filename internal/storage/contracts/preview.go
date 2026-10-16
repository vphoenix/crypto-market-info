package contracts

import (
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"time"
)

type PreviewBook struct {
	InstrumentID uint32
	StoredDepth  uint8
	Valid        bool
	Anchored     bool
	Snapshot     model.BookSnapshot
}
type PreviewDerivative struct {
	InstrumentID uint32
	Snapshot     model.DerivativeSnapshot
	Quality      model.DerivativeQuality
}
type PreviewIndex struct {
	IndexID string
	Sample  options.IndexSample
}
type PreviewFrame struct {
	At          time.Time
	Scope       string
	Kind        string
	Books       []PreviewBook
	Derivatives []PreviewDerivative
	Indexes     []PreviewIndex
}

// OfferPreview transfers an immutable sample without blocking sampling. Preview
// drops are permitted and cannot alter the sealed minute or its valid bitmap.
type PreviewSink interface{ OfferPreview(PreviewFrame) }
