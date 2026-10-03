package justlendkeeper

import (
	"errors"
	"reflect"
)

// MergeReceipt only complements missing protocol values; source timing and
// response packaging are observations, not immutable chain facts.
func MergeReceipt(a, b TxReceipt) (TxReceipt, error) {
	if a.BlockHash != b.BlockHash || a.BlockNumber != b.BlockNumber || a.TxId != b.TxId || !a.BlockTime.Equal(b.BlockTime) {
		return b, errors.New("canonical_receipt_anchor_conflict")
	}
	av, bv := reflect.ValueOf(a), reflect.ValueOf(&b).Elem()
	for _, name := range []string{"Sender", "OuterTarget", "OuterSelector", "OuterCallValueSun", "FeeSun", "EnergyUsageTotal", "EnergyUsage", "OriginEnergyUsage", "EnergyFeeSun", "EnergyPenaltyTotal", "NetUsage", "NetFeeSun", "MultisignFeeSun", "MemoFeeSun"} {
		x, y := av.FieldByName(name), bv.FieldByName(name)
		if !x.IsNil() && !y.IsNil() {
			if !reflect.DeepEqual(x.Interface(), y.Interface()) {
				return b, errors.New("canonical_receipt_value_conflict_" + name)
			}
		} else if y.IsNil() && !x.IsNil() {
			y.Set(x)
		}
	}
	if a.ExecutionResult != "unknown" && b.ExecutionResult != "unknown" && a.ExecutionResult != b.ExecutionResult {
		return b, errors.New("canonical_receipt_execution_conflict")
	}
	if b.ExecutionResult == "unknown" {
		b.ExecutionResult = a.ExecutionResult
	}
	if a.BodyComplete && b.BodyComplete && a.CallClass != b.CallClass {
		return b, errors.New("canonical_receipt_call_conflict")
	}
	if !b.BodyComplete && a.BodyComplete {
		b.CallClass = a.CallClass
		b.BodyComplete = true
		b.BodyPayloadHash = a.BodyPayloadHash
	}
	if a.NativeTransferStatus == "present" && b.NativeTransferStatus == "present" {
		x, _ := Freeze(a.NativeTransfers)
		y, _ := Freeze(b.NativeTransfers)
		if string(x) != string(y) {
			return b, errors.New("canonical_receipt_transfer_conflict")
		}
	} else if b.NativeTransferStatus != "present" && a.NativeTransferStatus == "present" {
		b.NativeTransferStatus = a.NativeTransferStatus
		b.NativeTransfers = a.NativeTransfers
	}
	if a.ReceiptComplete && b.ReceiptComplete && (a.RentalLiquidationLogCount != b.RentalLiquidationLogCount || a.OtherLogCount != b.OtherLogCount) {
		return b, errors.New("canonical_receipt_log_count_conflict")
	}
	return b, nil
}
