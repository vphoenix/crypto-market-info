package runtimecheckpoint

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type pageTask struct {
	Identity  string
	Pending   bool
	Payload   string
	KnownZero *uint64
}

func TestPagedStateKeepsAllTasksWithBoundedResidentCache(t *testing.T) {
	dir := t.TempDir()
	b := testBinding()
	files, err := Create(dir, b, []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	eligible := func(v pageTask) bool { return v.Pending }
	m, err := OpenPagedMap[pageTask](files, PageRoot{}, 256<<10, eligible)
	if err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	for i := 999; i >= 0; i-- {
		key := fmt.Sprintf("task-%05d", i)
		if err = m.Set(key, pageTask{Identity: key, Pending: i%3 == 0, Payload: strings.Repeat("p", 1024), KnownZero: &zero}); err != nil {
			t.Fatal(i, err)
		}
	}
	root, refs, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalTyped(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = files.Checkpoint(raw, refs); err != nil {
		t.Fatal(err)
	}
	if m.Len() != 1000 || m.Eligible() != 334 || m.Metrics().ResidentBytes > 256<<10 || len(refs) < 8 {
		t.Fatal(m.Len(), m.Eligible(), m.Metrics(), len(refs))
	}
	files.Close()
	files, err = Open(dir, b, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err = UnmarshalTyped(files.State(), &root); err != nil {
		t.Fatal(err)
	}
	m, err = OpenPagedMap[pageTask](files, root, 256<<10, eligible)
	if err != nil {
		t.Fatal(err)
	}
	if m.Metrics().PagesRead != 0 || m.Metrics().ResidentBytes != 0 {
		t.Fatal("eagerly loaded all work")
	}
	count := 0
	after := ""
	for {
		keys, values, err := m.Scan(after, 17, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) == 0 {
			break
		}
		for i, key := range keys {
			if key != values[i].Identity || values[i].KnownZero == nil || *values[i].KnownZero != 0 {
				t.Fatal(values[i])
			}
			if err = m.Delete(key); err != nil {
				t.Fatal(err)
			}
			count++
		}
		after = keys[len(keys)-1]
	}
	if count != 334 || m.Len() != 666 || m.Eligible() != 0 {
		t.Fatal(count, m.Len(), m.Eligible())
	}
	root, refs, err = m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err = MarshalTyped(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = files.Checkpoint(raw, refs); err != nil {
		t.Fatal(err)
	}
	// The previous image still references all 1000 tasks after current cleanup.
	recovery := filepath.Join(t.TempDir(), "previous")
	files.Close()
	if err = RecoverPrevious(dir, recovery, b, 0); err != nil {
		t.Fatal(err)
	}
	prior, err := Open(recovery, b, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer prior.Close()
	if err = UnmarshalTyped(prior.State(), &root); err != nil {
		t.Fatal(err)
	}
	old, err := OpenPagedMap[pageTask](prior, root, 256<<10, eligible)
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err := old.Get("task-00000")
	if err != nil || !exists || !value.Pending || old.Len() != 1000 {
		t.Fatal(value, exists, err, old.Len())
	}
}

func TestPagedStateNoChangesDoNotRewritePages(t *testing.T) {
	files, err := Create(t.TempDir(), testBinding(), []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	m, err := OpenPagedMap[int](files, PageRoot{}, 256<<10, func(v int) bool { return v > 0 })
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	first, refs, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	before := files.Metrics()
	second, again, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(refs, again) || files.Metrics().FileSyncs != before.FileSyncs {
		t.Fatal("unchanged page rewritten")
	}
	first.Pages[0].Eligible = 0
	bad, err := OpenPagedMap[int](files, first, 256<<10, func(v int) bool { return v > 0 })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = bad.Get("a"); err == nil {
		t.Fatal("tampered scheduling summary accepted")
	}
}

func TestTypedMemoryEstimateCountsPendingObjectsAndSharedPointers(t *testing.T) {
	value := strings.Repeat("x", 1024)
	if n := EstimateTypedBytes(map[string]*string{"a": &value, "b": &value}); n < 1024 || n > 2048 {
		t.Fatal(n)
	}
	if n := EstimateTypedBytes(make([]byte, 100, 1000)); n < 1000 {
		t.Fatal(n)
	}
}
