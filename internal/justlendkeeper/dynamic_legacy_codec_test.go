package justlendkeeper

import (
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// These are the deployed 1aacedc35 layouts. Presence-tree indexes are part of
// the recovery format even though gob identifies the body fields by name.
var legacyStateFields = strings.Fields("Runtime SolidAnchors CohortChanges BackfillActive BackfillFrom BackfillTo HistoryFrom HistoryTo HistoryCursor Version Target ConfigHash Day Used NextGlobal NextBackground Sources Scans Candidates Cohort CohortId Rotation BootstrapReady Ops NextEvents NextProbe NextCosts NextQuote NextIdentity Implementation CodeHash IdentityAt IdentityStatus CallersValid EventCursors RescanDay LastSolidNumber LastSolidHash IndexingEnabled EvidenceCursors NextEnrichment")
var legacyOperationFields = strings.Fields("RuntimeApplied Discoveries Kind ScanID Requests Evidence Batch RawEvents Headers Receipts NextFingerprint Frozen Failed NotBefore Candidate IdentityAddress IdentityCode Scheduled Caller ValidCallers ExcludedBoundary ExcludedResource Parent")
var legacyBatchFields = strings.Fields("Capture Events Receipts Probes Costs IndexedEvents IndexPage")
var legacyRequestFields = strings.Fields("Source Path Body Role Key Background Attempts NotBefore")

func legacyValue(v reflect.Value, names []string) reflect.Value {
	fields := make([]reflect.StructField, len(names))
	values := make([]reflect.Value, len(names))
	for i, name := range names {
		field, _ := v.Type().FieldByName(name)
		value := v.FieldByName(name)
		switch name {
		case "Ops", "Requests":
			layout := legacyOperationFields
			if name == "Requests" {
				layout = legacyRequestFields
			}
			prototype := legacyValue(reflect.New(value.Type().Elem()).Elem(), layout)
			rows := reflect.MakeSlice(reflect.SliceOf(prototype.Type()), value.Len(), value.Len())
			for j := 0; j < value.Len(); j++ {
				rows.Index(j).Set(legacyValue(value.Index(j), layout))
			}
			value = rows
			field.Type = rows.Type()
		case "Batch":
			value = legacyValue(value, legacyBatchFields)
			field.Type = value.Type()
		}
		fields[i] = field
		values[i] = value
	}
	result := reflect.New(reflect.StructOf(fields)).Elem()
	for i, value := range values {
		result.Field(i).Set(value)
	}
	return result
}
func TestDynamicReadsDeployedV1PresenceLayout(t *testing.T) {
	c, _, now := testCollector(t)
	keeperRuntimeFixture(t, c)
	c.State.Day = "2026-10-23"
	c.State.Used = 321
	o := c.NewOperation("probe", "live", "publicnode")
	o.Batch.Probes = []Probe{{ApiSuccess: Ptr(false), RewardReturnSun: big.NewInt(0)}}
	o.Parent = &Capture{RequestedFrom: Ptr(*now)}
	o.Requests = []Request{{Source: "publicnode", Attempts: 1}}
	c.State.Ops = []Operation{o}
	state := legacyValue(reflect.ValueOf(*c.State), legacyStateFields)
	fields := []reflect.StructField{{Name: "Version", Type: reflect.TypeOf(uint32(0))}, {Name: "State", Type: state.Type()}, {Name: "Candidates", Type: reflect.TypeOf(runtimecheckpoint.Reference{})}}
	legacy := reflect.New(reflect.StructOf(fields)).Elem()
	legacy.Field(0).SetUint(1)
	legacy.Field(1).Set(state)
	legacy.Field(2).Set(reflect.ValueOf(c.runtime.last.Candidates))
	raw, err := runtimeCompress(legacy.Interface())
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtimeDecode(raw, c.runtime.files, true)
	if err != nil {
		t.Fatal("deployed snapshot cannot be recovered", err)
	}
	if got.State.Runtime == nil || got.State.Dynamic != nil || got.State.Scheduler != nil || got.State.Used != 321 || got.State.Day != "2026-10-23" {
		t.Fatal("legacy state or budget changed")
	}
	p := got.State.Ops[0].Batch.Probes[0]
	if p.ApiSuccess == nil || *p.ApiSuccess || p.RewardReturnSun == nil || p.RewardReturnSun.Sign() != 0 || got.State.Ops[0].Parent == nil {
		t.Fatal("legacy pointer presence changed")
	}
}
