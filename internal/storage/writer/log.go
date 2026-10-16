package writer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DiagnosticLog keeps two bounded files. Rotation affects diagnostics only;
// durable queues and market facts are never pruned by this writer.
type DiagnosticLog struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	size, limit int64
}

func OpenDiagnosticLog(path string, limit int64) (*DiagnosticLog, error) {
	if !filepath.IsAbs(path) || limit < 1024 {
		return nil, fmt.Errorf("diagnostic log needs an absolute path and finite budget")
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	st, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return nil, e
	}
	return &DiagnosticLog{path: path, file: f, size: st.Size(), limit: limit}, nil
}
func (l *DiagnosticLog) Close() error { l.mu.Lock(); defer l.mu.Unlock(); return l.file.Close() }
func (l *DiagnosticLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if int64(len(p)) > l.limit {
		return 0, fmt.Errorf("diagnostic record exceeds log budget")
	}
	if l.size+int64(len(p)) > l.limit {
		if e := l.file.Close(); e != nil {
			return 0, e
		}
		if e := os.Rename(l.path, l.path+".1"); e != nil {
			return 0, e
		}
		f, e := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if e != nil {
			return 0, e
		}
		l.file = f
		l.size = 0
	}
	n, e := l.file.Write(p)
	l.size += int64(n)
	return n, e
}
