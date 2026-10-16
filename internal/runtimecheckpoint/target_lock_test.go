package runtimecheckpoint

import "testing"

func TestTargetLockIgnoresManifestAndEvidenceAliases(t *testing.T) {
	b := testBinding()
	b.Database = t.Name()
	first, err := LockTarget(b)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	alias := b
	alias.ScopeHash = "another-manifest"
	alias.Holder = "another-holder"
	alias.ProducerGeneration++
	if f, err := LockTarget(alias); err == nil {
		f.Close()
		t.Fatal("same target admitted two writers")
	}
	other := b
	other.Database += "-separate"
	f, err := LockTarget(other)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}
