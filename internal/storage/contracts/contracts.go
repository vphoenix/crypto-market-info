// Package contracts defines storage boundaries without database drivers.
package contracts

import (
	"context"
	"errors"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

var (
	ErrNotFound        = errors.New("market data not found")
	ErrConflict        = errors.New("immutable storage content conflict")
	ErrCapacity        = errors.New("durable storage queue capacity exhausted")
	ErrFenced          = errors.New("storage producer generation fenced")
	ErrHistoryNotReady = errors.New("historical range is not verified")
)

type InstrumentRegistry interface {
	Instruments(context.Context) ([]model.Instrument, error)
	RegisterInstruments(context.Context, []model.Instrument) ([]model.Instrument, error)
}
type BookReader interface {
	LoadMinute(context.Context, uint32, time.Time) (model.MinuteBook, error)
	LoadDeltas(context.Context, uint64, uint8) ([]model.BookDelta, error)
	ReplayBook(context.Context, uint32, time.Time) (model.BookSnapshot, bool, error)
}
type BookWriter interface {
	WriteCompletedMinute(context.Context, model.CompletedMinute) error
}
type FundingStore interface {
	UpsertFundingRate(context.Context, model.FundingRate) error
	LoadPendingFundingConfirmations(context.Context, time.Time, time.Time) ([]model.PendingFundingConfirmation, error)
}

// Delivery identity is frozen before durable acceptance. Payload is a typed
// envelope, never raw source JSON. Digest covers the complete immutable payload.
// Accept returning nil means durable queue ownership; Commit returning nil
// means the target transaction and matching commit record are durable.
type Delivery struct {
	Database   string
	Kind       string
	ID         string
	Service    string
	Generation uint64
	Minute     time.Time
	Digest     [32]byte
	Members    uint32
	Codec      uint16
	Payload    []byte
}

// Candidate carries only a frozen header. A consumer loads the payload after
// selecting and claiming the task; Bytes is the declared payload size.
type Candidate struct {
	Header Delivery
	Bytes  int64
}

func (d Delivery) Identity() string { return d.Database + "\x00" + d.Kind + "\x00" + d.ID }
func (d Delivery) SameHeader(other Delivery) bool {
	return d.Database == other.Database && d.Kind == other.Kind && d.ID == other.ID && d.Service == other.Service && d.Generation == other.Generation && d.Minute.Equal(other.Minute) && d.Digest == other.Digest && d.Members == other.Members && d.Codec == other.Codec
}

type DurableQueue interface {
	Accept(context.Context, Delivery) error
}
type DeliveryStore interface {
	Commit(context.Context, Delivery) error
	Committed(context.Context, Delivery) (bool, error)
}
