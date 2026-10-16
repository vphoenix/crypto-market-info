package writer

import (
	"fmt"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestSchedulerAccountsTimeAndServesBothAgesAndKinds(t *testing.T) {
	s := newScheduler()
	now := time.Now().UTC()
	var spent [2]time.Duration
	var cex [2]int
	for g := range 2 {
		for n := range 1000 {
			kind := "options_catalog"
			if n%8 == 0 {
				kind = "cex"
			}
			minute := now.Truncate(time.Minute).Add(-time.Minute)
			if g == 1 {
				minute = minute.Add(-time.Hour)
			}
			d := contracts.Delivery{Database: "db", Kind: kind, ID: fmt.Sprint(g, "/", n), Minute: minute, Members: 32}
			s.tasks[d.Identity()] = &task{candidate: contracts.Candidate{Header: d, Bytes: 100000}, disk: g == 1, redis: g == 0}
		}
	}
	for range 500 {
		task := s.pick(now)
		if task == nil {
			t.Fatal("no eligible task")
		}
		elapsed := 100 * time.Millisecond
		if task.candidate.Header.Kind == "cex" {
			elapsed = 10 * time.Millisecond
			cex[task.group]++
		}
		spent[task.group] += elapsed
		s.finish(task, elapsed)
		delete(s.tasks, task.candidate.Header.Identity())
	}
	share := float64(spent[1]) / float64(spent[0]+spent[1])
	if share < 0.27 || share > 0.33 || cex[0] == 0 || cex[1] == 0 {
		t.Fatal("age or kind starvation", share, cex)
	}
}
