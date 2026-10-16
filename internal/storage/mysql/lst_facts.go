package mysql

import (
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"reflect"
)

func lstEventFact(row any) string {
	v := reflect.ValueOf(row)
	copy := reflect.New(v.Type()).Elem()
	copy.Set(v)
	for _, name := range []string{"CaptureId", "EventPayloadHash", "AvailableAt", "ReceiptStatus", "TransactionSender", "TransactionTo", "InputSelector", "TransactionOperationCount", "GasSampleClass", "GasUsed", "EffectiveGasPriceWei", "ReceiptPayloadHash", "ReceiptAvailableAt", "RowHash"} {
		f := copy.FieldByName(name)
		if f.IsValid() {
			f.SetZero()
		}
	}
	return lst.CanonicalHash(copy.Interface())
}
func lstReceiptCompatible(a, b any) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	for _, name := range []string{"ReceiptStatus", "TransactionSender", "TransactionTo", "InputSelector", "TransactionOperationCount", "GasUsed", "EffectiveGasPriceWei"} {
		x, y := av.FieldByName(name), bv.FieldByName(name)
		if x.IsValid() && y.IsValid() && !x.IsNil() && !y.IsNil() && lst.CanonicalHash(x.Interface()) != lst.CanonicalHash(y.Interface()) {
			return false
		}
	}
	return true
}
