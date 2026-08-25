package solana

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

type Fee struct {
	Denominator uint64
	Numerator   uint64
}

type StakePoolState struct {
	PoolMint                 [32]byte
	TotalLamports            uint64
	PoolTokenSupply          uint64
	LastUpdateEpoch          uint64
	EpochFee                 Fee
	StakeWithdrawalFee       Fee
	SOLDepositFee            Fee
	LastEpochPoolTokenSupply uint64
	LastEpochTotalLamports   uint64
}

type PoolConfig struct {
	Address string
	Mint    string
}

type PoolSnapshot struct {
	State       StakePoolState
	Slot        uint64
	BlockHeight uint64
	BlockHash   string
	BlockTime   int64
	Payloads    []marketyield.Payload
}

type PoolReader interface {
	Read(context.Context, PoolConfig) (PoolSnapshot, error)
}

type Reader struct{ Client *Client }

type epochInfo struct {
	Epoch json.Number `json:"epoch"`
}

type blockResult struct {
	BlockHeight json.Number `json:"blockHeight"`
	Blockhash   string      `json:"blockhash"`
	BlockTime   json.Number `json:"blockTime"`
}

func (r *Reader) Read(ctx context.Context, config PoolConfig) (PoolSnapshot, error) {
	if r == nil || r.Client == nil {
		return PoolSnapshot{}, fmt.Errorf("stake pool reader is not configured")
	}
	expectedMint, err := DecodePubkey(config.Mint)
	if err != nil {
		return PoolSnapshot{}, fmt.Errorf("pool mint: %w", err)
	}
	account, err := r.Client.AccountInfo(ctx, config.Address)
	if err != nil {
		return PoolSnapshot{}, err
	}
	if account.Owner != StakePoolProgram {
		return PoolSnapshot{}, fmt.Errorf("stake pool owner %q does not match program", account.Owner)
	}
	state, err := ParseStakePool(account.Data)
	if err != nil {
		return PoolSnapshot{}, err
	}
	if state.PoolMint != expectedMint {
		return PoolSnapshot{}, fmt.Errorf("stake pool mint does not match configured mint")
	}
	var epoch epochInfo
	epochPayload, err := r.Client.call(ctx, "getEpochInfo", []any{map[string]any{"commitment": "finalized", "minContextSlot": account.Slot}}, &epoch)
	if err != nil {
		return PoolSnapshot{}, err
	}
	epochNumber, err := parseUint(epoch.Epoch, "epoch")
	if err != nil {
		return PoolSnapshot{}, err
	}
	if state.LastUpdateEpoch != epochNumber {
		return PoolSnapshot{}, fmt.Errorf("stake pool last update epoch %d differs from current epoch %d", state.LastUpdateEpoch, epochNumber)
	}
	if state.TotalLamports == 0 || state.PoolTokenSupply == 0 || state.LastEpochTotalLamports == 0 || state.LastEpochPoolTokenSupply == 0 {
		return PoolSnapshot{}, fmt.Errorf("stake pool has zero current or previous totals")
	}
	var block blockResult
	blockPayload, err := r.Client.call(ctx, "getBlock", []any{account.Slot, map[string]any{"transactionDetails": "none", "rewards": false, "commitment": "finalized"}}, &block)
	if err != nil {
		return PoolSnapshot{}, err
	}
	height, err := parseUint(block.BlockHeight, "block height")
	if err != nil || block.Blockhash == "" {
		return PoolSnapshot{}, fmt.Errorf("block anchor is incomplete")
	}
	blockTime, err := block.BlockTime.Int64()
	if err != nil || blockTime <= 0 {
		return PoolSnapshot{}, fmt.Errorf("block time is invalid")
	}
	return PoolSnapshot{State: state, Slot: account.Slot, BlockHeight: height, BlockHash: block.Blockhash, BlockTime: blockTime,
		Payloads: []marketyield.Payload{{Name: "getAccountInfo", Body: account.Payload}, {Name: "getEpochInfo", Body: epochPayload}, {Name: "getBlock", Body: blockPayload}}}, nil
}

type borshReader struct {
	data   []byte
	offset int
}

func ParseStakePool(data []byte) (StakePoolState, error) {
	// Stake Pool accounts are allocated with Borsh's maximum packed size. The
	// actual serialization is variable-length because it contains Option and
	// FutureEpoch fields. The program deserializes this prefix unchecked; bytes
	// after it may contain stale data when a later serialization becomes shorter.
	if len(data) != 611 {
		return StakePoolState{}, fmt.Errorf("stake pool account length %d is not 611", len(data))
	}
	r := borshReader{data: data}
	accountType, err := r.u8()
	if err != nil || accountType != 1 {
		return StakePoolState{}, fmt.Errorf("invalid stake pool account type")
	}
	if err = r.skip(3 * 32); err != nil {
		return StakePoolState{}, err
	}
	if _, err = r.u8(); err != nil {
		return StakePoolState{}, err
	}
	if err = r.skip(2 * 32); err != nil {
		return StakePoolState{}, err
	}
	poolMint, err := r.pubkey()
	if err != nil {
		return StakePoolState{}, err
	}
	if err = r.skip(2 * 32); err != nil {
		return StakePoolState{}, err
	}
	total, err := r.u64()
	if err != nil {
		return StakePoolState{}, err
	}
	supply, err := r.u64()
	if err != nil {
		return StakePoolState{}, err
	}
	epoch, err := r.u64()
	if err != nil {
		return StakePoolState{}, err
	}
	if err = r.skip(8 + 8 + 32); err != nil {
		return StakePoolState{}, err
	}
	epochFee, err := r.fee()
	if err != nil {
		return StakePoolState{}, err
	}
	if err = r.futureFee(); err != nil {
		return StakePoolState{}, err
	}
	if err = r.optionPubkey(); err != nil {
		return StakePoolState{}, err
	}
	if err = r.optionPubkey(); err != nil {
		return StakePoolState{}, err
	}
	if _, err = r.fee(); err != nil {
		return StakePoolState{}, err
	}
	withdrawFee, err := r.fee()
	if err != nil {
		return StakePoolState{}, err
	}
	if err = r.futureFee(); err != nil {
		return StakePoolState{}, err
	}
	if _, err = r.u8(); err != nil {
		return StakePoolState{}, err
	}
	if err = r.optionPubkey(); err != nil {
		return StakePoolState{}, err
	}
	depositFee, err := r.fee()
	if err != nil {
		return StakePoolState{}, err
	}
	if _, err = r.u8(); err != nil {
		return StakePoolState{}, err
	}
	if err = r.optionPubkey(); err != nil {
		return StakePoolState{}, err
	}
	if _, err = r.fee(); err != nil {
		return StakePoolState{}, err
	}
	if err = r.futureFee(); err != nil {
		return StakePoolState{}, err
	}
	lastSupply, err := r.u64()
	if err != nil {
		return StakePoolState{}, err
	}
	lastTotal, err := r.u64()
	if err != nil {
		return StakePoolState{}, err
	}
	state := StakePoolState{PoolMint: poolMint, TotalLamports: total, PoolTokenSupply: supply, LastUpdateEpoch: epoch, EpochFee: epochFee,
		StakeWithdrawalFee: withdrawFee, SOLDepositFee: depositFee, LastEpochPoolTokenSupply: lastSupply, LastEpochTotalLamports: lastTotal}
	for name, fee := range map[string]Fee{"epoch fee": epochFee, "stake withdrawal fee": withdrawFee, "SOL deposit fee": depositFee} {
		if err = validateFee(fee); err != nil {
			return StakePoolState{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	return state, nil
}

func validateFee(fee Fee) error {
	if fee.Denominator == 0 && fee.Numerator != 0 {
		return fmt.Errorf("zero denominator requires zero numerator")
	}
	if fee.Denominator > 0 && fee.Numerator > fee.Denominator {
		return fmt.Errorf("numerator exceeds denominator")
	}
	return nil
}

func (r *borshReader) skip(size int) error {
	if size < 0 || r.offset+size > len(r.data) {
		return fmt.Errorf("truncated stake pool data at byte %d", r.offset)
	}
	r.offset += size
	return nil
}
func (r *borshReader) u8() (uint8, error) {
	if err := r.skip(1); err != nil {
		return 0, err
	}
	return r.data[r.offset-1], nil
}
func (r *borshReader) u64() (uint64, error) {
	start := r.offset
	if err := r.skip(8); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(r.data[start:r.offset]), nil
}
func (r *borshReader) pubkey() ([32]byte, error) {
	var value [32]byte
	start := r.offset
	if err := r.skip(32); err != nil {
		return value, err
	}
	copy(value[:], r.data[start:r.offset])
	return value, nil
}
func (r *borshReader) fee() (Fee, error) {
	denominator, err := r.u64()
	if err != nil {
		return Fee{}, err
	}
	numerator, err := r.u64()
	if err != nil {
		return Fee{}, err
	}
	fee := Fee{Denominator: denominator, Numerator: numerator}
	return fee, validateFee(fee)
}
func (r *borshReader) optionPubkey() error {
	tag, err := r.u8()
	if err != nil {
		return err
	}
	switch tag {
	case 0:
		return nil
	case 1:
		return r.skip(32)
	default:
		return fmt.Errorf("invalid optional pubkey tag %d", tag)
	}
}
func (r *borshReader) futureFee() error {
	tag, err := r.u8()
	if err != nil {
		return err
	}
	switch tag {
	case 0:
		return nil
	case 1, 2:
		_, err = r.fee()
		return err
	default:
		return fmt.Errorf("invalid future fee tag %d", tag)
	}
}
