package writer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticLogRotationIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.log")
	log, e := OpenDiagnosticLog(path, 1024)
	if e != nil {
		t.Fatal(e)
	}
	for range 20 {
		if _, e = log.Write(make([]byte, 200)); e != nil {
			t.Fatal(e)
		}
	}
	if e = log.Close(); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{path, path + ".1"} {
		st, e := os.Stat(name)
		if e != nil {
			t.Fatal(e)
		}
		if st.Size() > 1024 || st.Mode().Perm() != 0600 {
			t.Fatal("unbounded or public diagnostics", st.Size(), st.Mode())
		}
	}
}
