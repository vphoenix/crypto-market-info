package runtimecheckpoint

import (
	"errors"
	"sort"
	"time"
)

const pageItems = 128

// PageRoot is checkpoint metadata, not a history cache. Pages contain concrete
// domain records keyed by their existing task identity. Only a bounded cache is
// resident; checkpoint references retain both current and previous page images.
type PageRoot struct {
	Version     uint32
	Next, Count uint64
	Pages       []PageDescriptor
}
type PageDescriptor struct {
	ID              uint64
	First, Last     string
	Count, Eligible uint32
	Object          Reference
	Oldest          int64
}
type pageItem[T any] struct {
	Key   string
	Value T
}
type typedPage[T any] struct {
	Version uint32
	Items   []pageItem[T]
}
type residentPage[T any] struct {
	Value typedPage[T]
	Dirty bool
	Used  uint64
	Bytes int64
}
type PageMetrics struct {
	ResidentBytes, PeakResidentBytes int64
	PagesRead, PagesWritten          uint64
}

// MemoryBytes estimates live page headers, cached values and cache bookkeeping.
// Payload files outside these pages are counted separately by the domain.
func (m *PagedMap[T]) MemoryBytes() int64 {
	return EstimateTypedBytes(m.root) + m.metrics.ResidentBytes + 128*int64(len(m.cache))
}

// PagedMap has one owner and no background I/O. Eligibility is the domain's
// scheduling predicate; zero-eligible pages need not be loaded for selection.
// Values passed to or returned from it must not be mutated outside this owner.
type PagedMap[T any] struct {
	files    *Store
	root     PageRoot
	cache    map[uint64]*residentPage[T]
	eligible func(T) bool
	oldest   func(T) time.Time
	limit    int64
	clock    uint64
	metrics  PageMetrics
}

func OpenPagedMap[T any](files *Store, root PageRoot, cacheBytes int64, eligible func(T) bool, oldest ...func(T) time.Time) (*PagedMap[T], error) {
	if cacheBytes < 256<<10 || eligible == nil {
		return nil, errors.New("paged state requires cache budget and domain predicate")
	}
	if root.Version == 0 && root.Count == 0 && len(root.Pages) == 0 {
		root.Version = 1
	}
	if root.Version != 1 {
		return nil, ErrRecovery
	}
	var count uint64
	ids := map[uint64]bool{}
	for i, p := range root.Pages {
		if p.ID == 0 || p.ID > root.Next || ids[p.ID] || p.Count == 0 || p.Count > pageItems || p.Eligible > p.Count || p.First == "" || p.First > p.Last || i > 0 && root.Pages[i-1].Last >= p.First {
			return nil, ErrRecovery
		}
		if _, err := files.objectPath(p.Object); err != nil {
			return nil, err
		}
		count += uint64(p.Count)
		ids[p.ID] = true
	}
	if count != root.Count {
		return nil, ErrRecovery
	}
	root.Pages = append([]PageDescriptor(nil), root.Pages...)
	m := &PagedMap[T]{files: files, root: root, cache: map[uint64]*residentPage[T]{}, eligible: eligible, limit: cacheBytes}
	if len(oldest) > 0 {
		m.oldest = oldest[0]
	}
	return m, nil
}

func (m *PagedMap[T]) index(key string) int {
	i := sort.Search(len(m.root.Pages), func(i int) bool { return m.root.Pages[i].Last >= key })
	if i == len(m.root.Pages) && i > 0 {
		i--
	}
	return i
}
func (m *PagedMap[T]) descriptor(id uint64) *PageDescriptor {
	for i := range m.root.Pages {
		if m.root.Pages[i].ID == id {
			return &m.root.Pages[i]
		}
	}
	return nil
}
func (m *PagedMap[T]) load(i int) (*residentPage[T], error) {
	d := m.root.Pages[i]
	m.clock++
	if p := m.cache[d.ID]; p != nil {
		p.Used = m.clock
		return p, nil
	}
	raw, err := m.files.ReadObject(d.Object)
	if err != nil {
		return nil, err
	}
	var value typedPage[T]
	if err = UnmarshalTyped(raw, &value); err != nil {
		return nil, err
	}
	if value.Version != 1 || len(value.Items) != int(d.Count) {
		return nil, ErrRecovery
	}
	var eligible uint32
	var oldest int64
	for j, item := range value.Items {
		if item.Key == "" || j > 0 && value.Items[j-1].Key >= item.Key {
			return nil, ErrRecovery
		}
		if m.eligible(item.Value) {
			eligible++
		}
		if m.oldest != nil {
			at := m.oldest(item.Value)
			if !at.IsZero() && (oldest == 0 || at.UnixNano() < oldest) {
				oldest = at.UnixNano()
			}
		}
	}
	if value.Items[0].Key != d.First || value.Items[len(value.Items)-1].Key != d.Last || eligible != d.Eligible || oldest != d.Oldest {
		return nil, ErrRecovery
	}
	p := &residentPage[T]{Value: value, Used: m.clock, Bytes: EstimateTypedBytes(value)}
	if p.Bytes > m.limit {
		return nil, ErrCapacity
	}
	if err = m.makeRoom(p.Bytes, 0); err != nil {
		return nil, err
	}
	m.cache[d.ID] = p
	m.metrics.PagesRead++
	m.account()
	return p, nil
}
func (m *PagedMap[T]) account() {
	m.metrics.ResidentBytes = 0
	for _, p := range m.cache {
		m.metrics.ResidentBytes += p.Bytes
	}
	m.metrics.PeakResidentBytes = max(m.metrics.PeakResidentBytes, m.metrics.ResidentBytes)
}
func (m *PagedMap[T]) flush(id uint64, p *residentPage[T]) error {
	if !p.Dirty {
		return nil
	}
	raw, err := MarshalTyped(p.Value)
	if err != nil {
		return err
	}
	ref, err := m.files.PutObject(raw)
	if err != nil {
		return err
	}
	d := m.descriptor(id)
	if d == nil {
		return ErrRecovery
	}
	d.Object = ref
	p.Dirty = false
	m.metrics.PagesWritten++
	return nil
}
func (m *PagedMap[T]) makeRoom(extra int64, pinned uint64) error {
	for {
		m.account()
		if m.metrics.ResidentBytes+extra <= m.limit {
			return nil
		}
		var victim uint64
		var oldest uint64 = ^uint64(0)
		for id, p := range m.cache {
			if id != pinned && p.Used < oldest {
				victim, oldest = id, p.Used
			}
		}
		if victim == 0 {
			return ErrCapacity
		}
		if err := m.flush(victim, m.cache[victim]); err != nil {
			return err
		}
		delete(m.cache, victim)
	}
}
func (m *PagedMap[T]) update(d *PageDescriptor, p *residentPage[T]) {
	d.Count = uint32(len(p.Value.Items))
	d.Eligible = 0
	d.Oldest = 0
	d.First = p.Value.Items[0].Key
	d.Last = p.Value.Items[len(p.Value.Items)-1].Key
	for _, item := range p.Value.Items {
		if m.eligible(item.Value) {
			d.Eligible++
		}
		if m.oldest != nil {
			at := m.oldest(item.Value)
			if !at.IsZero() && (d.Oldest == 0 || at.UnixNano() < d.Oldest) {
				d.Oldest = at.UnixNano()
			}
		}
	}
	p.Dirty = true
	p.Bytes = EstimateTypedBytes(p.Value)
	m.clock++
	p.Used = m.clock
}
func (m *PagedMap[T]) Get(key string) (T, bool, error) {
	var zero T
	if len(m.root.Pages) == 0 {
		return zero, false, nil
	}
	i := m.index(key)
	d := m.root.Pages[i]
	if key < d.First || key > d.Last {
		return zero, false, nil
	}
	p, err := m.load(i)
	if err != nil {
		return zero, false, err
	}
	j := sort.Search(len(p.Value.Items), func(j int) bool { return p.Value.Items[j].Key >= key })
	if j == len(p.Value.Items) || p.Value.Items[j].Key != key {
		return zero, false, nil
	}
	return p.Value.Items[j].Value, true, nil
}
func (m *PagedMap[T]) Set(key string, value T) error {
	if key == "" || len(key) > 1024 {
		return ErrCapacity
	}
	if len(m.root.Pages) == 0 {
		m.root.Next++
		m.root.Pages = []PageDescriptor{{ID: m.root.Next}}
		m.cache[m.root.Next] = &residentPage[T]{Value: typedPage[T]{Version: 1}}
	}
	i := m.index(key)
	id := m.root.Pages[i].ID
	p, err := m.load(i)
	if err != nil {
		return err
	}
	j := sort.Search(len(p.Value.Items), func(j int) bool { return p.Value.Items[j].Key >= key })
	if j < len(p.Value.Items) && p.Value.Items[j].Key == key {
		p.Value.Items[j].Value = value
	} else {
		p.Value.Items = append(p.Value.Items, pageItem[T]{})
		copy(p.Value.Items[j+1:], p.Value.Items[j:])
		p.Value.Items[j] = pageItem[T]{key, value}
		m.root.Count++
	}
	m.update(&m.root.Pages[i], p)
	if len(p.Value.Items) > pageItems || p.Bytes > m.limit/2 && len(p.Value.Items) > 1 {
		middle := len(p.Value.Items) / 2
		right := &residentPage[T]{Value: typedPage[T]{Version: 1, Items: append([]pageItem[T](nil), p.Value.Items[middle:]...)}}
		p.Value.Items = append([]pageItem[T](nil), p.Value.Items[:middle]...)
		m.update(&m.root.Pages[i], p)
		if m.root.Next == ^uint64(0) {
			return ErrCapacity
		}
		m.root.Next++
		d := PageDescriptor{ID: m.root.Next}
		m.update(&d, right)
		m.root.Pages = append(m.root.Pages, PageDescriptor{})
		copy(m.root.Pages[i+2:], m.root.Pages[i+1:])
		m.root.Pages[i+1] = d
		// Flush the split page directly; it need not occupy a second cache slot.
		if err = m.flush(d.ID, right); err != nil {
			return err
		}
	}
	if p.Bytes > m.limit {
		return ErrCapacity
	}
	return m.makeRoom(0, id)
}
func (m *PagedMap[T]) Delete(key string) error {
	if len(m.root.Pages) == 0 {
		return nil
	}
	i := m.index(key)
	d := m.root.Pages[i]
	if key < d.First || key > d.Last {
		return nil
	}
	p, err := m.load(i)
	if err != nil {
		return err
	}
	j := sort.Search(len(p.Value.Items), func(j int) bool { return p.Value.Items[j].Key >= key })
	if j == len(p.Value.Items) || p.Value.Items[j].Key != key {
		return nil
	}
	copy(p.Value.Items[j:], p.Value.Items[j+1:])
	var zero pageItem[T]
	p.Value.Items[len(p.Value.Items)-1] = zero
	p.Value.Items = p.Value.Items[:len(p.Value.Items)-1]
	m.root.Count--
	if len(p.Value.Items) == 0 {
		delete(m.cache, d.ID)
		m.root.Pages = append(m.root.Pages[:i], m.root.Pages[i+1:]...)
		m.account()
		return nil
	}
	m.update(&m.root.Pages[i], p)
	m.account()
	return nil
}

// Scan returns at most limit tasks, strictly after the original identity. Its
// result is bounded independently of the total pending population. Callers may
// update/delete returned values, but must not mutate their nested references.
func (m *PagedMap[T]) Scan(after string, limit int, eligibleOnly bool) ([]string, []T, error) {
	if limit < 1 || limit > pageItems {
		return nil, nil, ErrCapacity
	}
	keys := make([]string, 0, limit)
	values := make([]T, 0, limit)
	for i, d := range m.root.Pages {
		if d.Last <= after || eligibleOnly && d.Eligible == 0 {
			continue
		}
		p, err := m.load(i)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range p.Value.Items {
			if item.Key > after && (!eligibleOnly || m.eligible(item.Value)) {
				keys = append(keys, item.Key)
				values = append(values, item.Value)
				if len(keys) == limit {
					return keys, values, nil
				}
			}
		}
	}
	return keys, values, nil
}
func (m *PagedMap[T]) Snapshot() (PageRoot, []Reference, error) {
	for id, p := range m.cache {
		if err := m.flush(id, p); err != nil {
			return PageRoot{}, nil, err
		}
	}
	root := m.root
	root.Pages = append([]PageDescriptor(nil), root.Pages...)
	refs := make([]Reference, 0, len(root.Pages))
	for _, p := range root.Pages {
		refs = append(refs, p.Object)
	}
	return root, refs, nil
}
func (m *PagedMap[T]) Len() uint64 { return m.root.Count }
func (m *PagedMap[T]) Eligible() uint64 {
	var n uint64
	for _, p := range m.root.Pages {
		n += uint64(p.Eligible)
	}
	return n
}
func (m *PagedMap[T]) Metrics() PageMetrics { m.account(); return m.metrics }

func (m *PagedMap[T]) Oldest() time.Time {
	var oldest int64
	for _, p := range m.root.Pages {
		if p.Oldest != 0 && (oldest == 0 || p.Oldest < oldest) {
			oldest = p.Oldest
		}
	}
	if oldest == 0 {
		return time.Time{}
	}
	return time.Unix(0, oldest).UTC()
}
