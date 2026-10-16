package contracts

import (
	"context"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"time"
)

type Lifecycle interface{ Close() error }
type SchemaReader interface{ InitSchema(context.Context) error }
type CanonicalStore interface {
	CanonicalMappings(context.Context, string) ([]model.CanonicalMapping, error)
	WriteCanonicalMappings(context.Context, []model.CanonicalMapping) error
	WritePerpetualUniverseRun(context.Context, model.PerpetualUniverseRun, []model.PerpetualUniverseMember) error
	PerpetualUniverse(context.Context, uuid.UUID) (model.PerpetualUniverseRun, []model.PerpetualUniverseMember, error)
	CanonicalMappingsForRun(context.Context, uuid.UUID) ([]model.CanonicalMapping, error)
}
type PairStore interface {
	InitOKXPairSchema(context.Context) error
	WriteOKXLoanPolicy(context.Context, model.OKXLoanPolicy) error
	WriteOKXPairCatalog(context.Context, model.OKXPairObservation) error
	LatestOKXLoanPolicy(context.Context, time.Time) (model.OKXLoanPolicy, error)
	LatestOKXPairCatalog(context.Context, time.Time) (model.OKXPairObservation, error)
}
type DiscoveryWriter interface {
	WriteDiscoveryFact(context.Context, model.DiscoveryFact) error
	WriteDiscoveryCapital(context.Context, model.CapitalParameters) error
}
type DiscoveryReader interface {
	LoadDiscoveryForecast(context.Context, uint32, time.Time) (model.FundingPrediction, DiscoveryProof, error)
	LoadDiscoveryBar(context.Context, uint32, time.Time) (model.TradeBar, DiscoveryProof, error)
	LoadDiscoveryFees(context.Context, uint32, time.Time) (model.ReferenceFee, DiscoveryProof, error)
	LoadDiscoveryCapital(context.Context, uint32, time.Time) (model.CapitalParameters, DiscoveryProof, error)
}
type YieldStore interface {
	InitYieldRegistry(context.Context) error
	WriteYieldBatch(context.Context, marketyield.Batch) error
}
type MainStore interface {
	Lifecycle
	SchemaReader
	InstrumentRegistry
	BookWriter
	BookReader
	FundingStore
	CanonicalStore
	PairStore
	DiscoveryWriter
	DiscoveryReader
	YieldStore
}
type OptionsStore interface {
	LoadDerivativeSpecs(context.Context, []uint32) ([]options.ContractSpec, error)
	LoadDerivativeTradingRule(context.Context, string) (options.TradingRule, error)
	Lifecycle
	InitOptionsLiveSchema(context.Context) error
	InitOptionsCatalogSchema(context.Context) error
	RegisterDerivativeSpecs(context.Context, []options.ContractSpec) ([]options.ContractSpec, error)
	WriteDerivativeTradingRule(context.Context, options.TradingRule) error
	WriteDerivativeTradingRules(context.Context, []options.TradingRule) error
	WriteOptionsMetadata(context.Context, options.MetadataObservation) error
	WriteOptionsRun(context.Context, options.LiveRun) error
	WriteOptionsMinute(context.Context, options.LiveEnvelope) error
	WriteOptionsCatalog(context.Context, options.CatalogObservation) error
	WriteOptionsLifecycle(context.Context, options.LifecycleObservation) error
	WriteOptionsPlan(context.Context, options.CollectionPlan) error
	LatestOptionsPlan(context.Context) (options.CollectionPlan, error)
	OptionsPlanAt(context.Context, time.Time) (options.CollectionPlan, error)
	LoadOptionsPlan(context.Context, uuid.UUID) (options.CollectionPlan, error)
	WriteOptionsCatalogMinute(context.Context, options.CatalogEnvelope) error
	WriteOptionsCatalogMinuteLazy(context.Context, uuid.UUID, time.Time, func() (options.CatalogEnvelope, error)) error
	OptionsStoreIdentity() string
	OptionsNotFound() error
	LatestOptionsRun(context.Context) (options.LiveRun, error)
	LoadOptionsRun(context.Context, uuid.UUID) (options.LiveRun, error)
	LoadOptionsMinute(context.Context, uuid.UUID, time.Time) (options.LiveEnvelope, error)
	LatestOptionsMinute(context.Context, uuid.UUID) (time.Time, error)
	HasOptionsCatalog(context.Context) (bool, error)
	LoadOptionsPlanSnapshot(context.Context, time.Time) (CatalogPlanSnapshot, error)
	LoadDerivativeBookEnvelope(context.Context, uuid.UUID, time.Time) (options.BookEnvelope, error)
}
type AcrossStore interface {
	Lifecycle
	across.Store
	InitAcrossSchema(context.Context) error
	AcrossBatches(context.Context, []across.Capture) (across.BatchLoad, error)
}
type ReserveStore interface {
	Lifecycle
	reserve.Store
	reserve.ReceiptDataStore
	reserve.SimulationStore
	InitReserveSchema(context.Context) error
	ReserveReceipts(context.Context) ([]dex.Receipt, error)
	ReserveBatches(context.Context, []reserve.Capture) (map[string]reserve.Batch, error)
}
type LSTStore interface {
	Lifecycle
	lst.Store
	InstrumentRegistry
	InitLSTSchema(context.Context) error
}
type KeeperStore interface {
	Lifecycle
	justlendkeeper.Store
	justlendkeeper.CollectionReader
	justlendkeeper.ReportReader
	justlendkeeper.BulkReportReader
	justlendkeeper.IndexPageReader
	justlendkeeper.IndexPageWriter
	InitKeeperSchema(context.Context) error
	KeeperParentCaptures(context.Context, []uuid.UUID) ([]justlendkeeper.Capture, error)
}
type DEXStore interface {
	Lifecycle
	InitDEXSchema(context.Context) error
	DEXBlocks(context.Context, time.Time, time.Time) ([]dex.Block, error)
	DEXBlockRange(context.Context, dex.Hash, uint64, uint64) ([]dex.Block, error)
	DEXLastBlock(context.Context, dex.Hash, bool) (*dex.Block, error)
	DEXQuotes(context.Context, []dex.Block) (map[dex.Hash][]dex.Quote, error)
	DEXLogs(context.Context, dex.Block) ([]dex.Log, error)
	DEXReceipts(context.Context, dex.Block) ([]dex.Receipt, error)
	DEXGapStart(context.Context, dex.Hash) (uint64, error)
	DEXReceiptHashes(context.Context, dex.Block) (map[dex.Hash]bool, error)
	DEXPendingReceiptBlocks(context.Context, dex.Hash) ([]dex.Block, error)
	DEXUnfinalizedBlocks(context.Context, dex.Hash, uint64) ([]dex.Block, error)
	DEXPendingLogBlocks(context.Context, dex.Hash) ([]dex.Block, error)
	WriteDEXBatch(context.Context, dex.Batch) error
	WriteDEXBlock(context.Context, dex.Block) error
	WriteDEXReceipts(context.Context, []dex.Receipt) error
	CompleteDEXLogs(context.Context, dex.Block, []dex.Log) error
}

type DiscoveryProof struct {
	Kind, ID, Hash, Codec                                   string
	SourceMS, ObservedMS, KnownMS, PublishedMS, EffectiveMS int64
	InstrumentID                                            uint32
	SourceCount                                             uint16
	ProvenanceHash                                          string
}

type CatalogPlanSnapshot struct {
	Plan                                    options.CollectionPlan
	MinuteTime                              time.Time
	ExpectedBooks, ValidBooks, MissingBooks int
	MissingRuns                             []uuid.UUID
	Runs                                    []options.LiveEnvelope
}

type PerpetualSourceCheck struct {
	InstrumentID         uint32     `json:"instrument_id"`
	Exchange             string     `json:"exchange"`
	Symbol               string     `json:"symbol"`
	CanonicalMarketKey   string     `json:"canonical_market_key"`
	MinuteRows           uint64     `json:"minute_rows"`
	ValidSeconds         uint64     `json:"valid_seconds"`
	RecentValidSeconds   uint64     `json:"recent_valid_seconds"`
	InvalidMinuteRows    uint64     `json:"invalid_minute_rows"`
	FirstMinute          *time.Time `json:"first_minute"`
	LatestMinute         *time.Time `json:"latest_minute"`
	ChangedPriceLevels   uint64     `json:"changed_price_levels"`
	FundingRows          uint64     `json:"funding_rows"`
	EstimatedFundingRows uint64     `json:"estimated_funding_rows"`
	ActualFundingRows    uint64     `json:"actual_funding_rows"`
	LatestFundingHour    *time.Time `json:"latest_funding_hour"`
}

type PerpetualPartCheck struct {
	Approximate         bool   `json:"approximate,omitempty"`
	Table               string `json:"table"`
	PhysicalRows        uint64 `json:"physical_rows"`
	DataCompressedBytes uint64 `json:"data_compressed_bytes"`
	BytesOnDisk         uint64 `json:"bytes_on_disk"`
}

type PerpetualDataCheck struct {
	Run                     model.PerpetualUniverseRun `json:"run"`
	RunSupersededAt         *time.Time                 `json:"run_superseded_at"`
	BookFrom                time.Time                  `json:"book_from"`
	BookThroughExclusive    time.Time                  `json:"book_through_exclusive"`
	FundingFrom             time.Time                  `json:"funding_from"`
	FundingThroughExclusive time.Time                  `json:"funding_through_exclusive"`
	RecentFrom              time.Time                  `json:"recent_from"`
	Sources                 []PerpetualSourceCheck     `json:"sources"`
	DatabaseParts           []PerpetualPartCheck       `json:"database_parts"`
}

type PerpetualReader interface {
	Lifecycle
	BookReader
	CanonicalStore
	CheckPerpetualData(context.Context, uuid.UUID, time.Time, time.Duration) (PerpetualDataCheck, error)
	PerpetualReplaySeconds(context.Context, uint32, time.Time, time.Time, int, int64) ([]time.Time, error)
}
