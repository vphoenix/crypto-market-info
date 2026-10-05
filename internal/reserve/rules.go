package reserve

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Keep small, immutable rules used to interpret data. These are configuration
// and compiled code, not API/RPC responses or an expanding response archive.
func SaveRules(dir string, m Manifest) error {
	identity, e := json.Marshal(m.Identity())
	if e != nil {
		return e
	}
	dir = filepath.Join(dir, m.Hash.String()[2:])
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	for name, raw := range map[string][]byte{
		"manifest.json": m.Raw, "identity.json": identity, "reserve-abi.json": []byte(abiJSON),
		"RouteProbe.sol": []byte(probeSource), "probe-abi.json": []byte(probeABIJSON), "probe-runtime.hex": []byte(probeRuntime), "compiler.json": []byte(probeCompiler),
	} {
		path := filepath.Join(dir, name)
		old, e := os.ReadFile(path)
		if e == nil {
			if !bytes.Equal(old, raw) {
				return errors.New("conflicting_rule_artifact")
			}
			continue
		}
		if !os.IsNotExist(e) {
			return e
		}
		if e = writeRule(path, raw); e != nil {
			return e
		}
	}
	return nil
}

func writeRule(path string, raw []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(raw); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
