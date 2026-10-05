package justlendkeeper

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

// Evidence is immutable and content-addressed. Read at most eight local files
// concurrently, within the same bounded capture group used by the SQL reader.
// All results must pass before any capture in the group is analyzed.
func evidenceParallel(ctx context.Context, count int, work func(int) error) error {
	errs := make([]error, count)
	var workers sync.WaitGroup
	for worker := 0; worker < min(8, count); worker++ {
		workers.Add(1)
		go func(start int) {
			defer workers.Done()
			for i := start; i < count; i += 8 {
				if errs[i] = ctx.Err(); errs[i] != nil {
					return
				}
				errs[i] = work(i)
			}
		}(worker)
	}
	workers.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func reportEvidence(ctx context.Context, a Archive, caps []Capture, verified map[string]bool, retain map[string]bool) (map[uuid.UUID]Manifest, map[string][]byte, error) {
	if len(caps) > 256 {
		return nil, nil, errors.New("keeper_report_batch_too_large")
	}
	manifests := make([]Manifest, len(caps))
	if err := evidenceParallel(ctx, len(caps), func(i int) error {
		var err error
		manifests[i], err = readManifest(a, caps[i])
		return err
	}); err != nil {
		return nil, nil, err
	}
	needed := map[string]bool{}
	for _, m := range manifests {
		for _, request := range m.Requests {
			for _, hash := range []string{request.RequestHash, request.ResponseHash} {
				if hash == "" {
					continue
				}
				h, err := BinaryHex(hash, 32)
				if err != nil {
					return nil, nil, err
				}
				if !verified[h] {
					needed[h] = true
				}
			}
		}
	}
	for h := range retain {
		needed[h] = true
	}
	hashes := make([]string, 0, len(needed))
	for h := range needed {
		hashes = append(hashes, h)
	}
	retained := make([][]byte, len(hashes))
	var retainedMu sync.Mutex
	retainedBytes := 0
	if err := evidenceParallel(ctx, len(hashes), func(i int) error {
		body, err := a.Get(hashes[i])
		if err == nil && retain[hashes[i]] {
			retainedMu.Lock()
			// Larger responses fall back to a sequential authenticated reread.
			if len(body) <= 16*1024*1024-retainedBytes {
				retained[i] = body
				retainedBytes += len(body)
			}
			retainedMu.Unlock()
		}
		return err
	}); err != nil {
		return nil, nil, err
	}
	result := make(map[uuid.UUID]Manifest, len(caps))
	for i, cap := range caps {
		result[cap.CaptureId] = manifests[i]
	}
	bodies := make(map[string][]byte, len(retain))
	for i, h := range hashes {
		verified[h] = true
		if retained[i] != nil {
			bodies[h] = retained[i]
		}
	}
	return result, bodies, nil
}
