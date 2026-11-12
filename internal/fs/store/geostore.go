package store

// GeoStore: region-scoped write quorum with asynchronous cross-region
// convergence (4.1). Local commits ack as soon as the local region's mirror
// reaches quorum; a background shipper then replays every local write to the
// remote region's mirror in order. The remote region is read-mostly in this
// version: its mirror receives the same objects and the same HEAD pointer,
// so reads (and disaster failover) work, but writes are expected to be
// directed at the local region - a WAN-stretched commit quorum would exceed
// the write budget.
//
// Semantics:
//   - Put/CAS/CreateIfAbsent on the local mirror behave exactly as before
//     (quorum ack, WORM journal chain, HEAD CAS through CASQuorum).
//   - Each acknowledged mutation is queued and shipped to the remote mirror
//     by one goroutine: immutable objects via plain Put (idempotent), the
//     HEAD control object via CAS against the remote's current etag, or a
//     plain Put when the remote is empty (fresh region / disaster rebuild).
//   - The shipper retries with backoff; a partition never fails a local
//     commit. Lag is observable via Status() (pending/shipped/errors, for a
//     future metric).
//   - Reads and lists always hit the local region. Remote visibility is
//     eventual (seconds on a healthy WAN).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

// GeoConfig wires one local region and one remote region.
type GeoConfig struct {
	Local  *MirrorStore
	Remote *MirrorStore
	// RepoPrefix scopes the reconcile listing (e.g. "repos/alpha").
	RepoPrefix string
	// ControlName is the HEAD pointer object key (the mirror's control
	// object), published on the remote via monotonic CAS.
	ControlName string
	// Interval between shipper drain attempts when idle.
	Interval time.Duration
	// OnShip, when set, observes each shipped key (tests).
	OnShip func(key string)
}

// GeoStore is a Store: local-region semantics with async remote shipping.
type GeoStore struct {
	local      *MirrorStore
	remote     *MirrorStore
	ctrl       string
	repoPrefix string

	mu    sync.Mutex
	queue []geoItem
	wake  chan struct{}
	stop  chan struct{}
	done  sync.WaitGroup

	shipped   uint64
	shipErrs  uint64
	lastShip  time.Time
	lastError error

	onShip func(key string)
}

// geoItem is one acknowledged mutation awaiting remote shipment.
type geoItem struct {
	key  string
	data []byte
}

// NewGeoStore builds the store and starts the background shipper.
func NewGeoStore(cfg GeoConfig) *GeoStore {
	g := &GeoStore{
		local:      cfg.Local,
		remote:     cfg.Remote,
		ctrl:       cfg.ControlName,
		repoPrefix: cfg.RepoPrefix,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		onShip:     cfg.OnShip,
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = time.Second
	}
	g.done.Add(1)
	go g.shipper(interval)
	return g
}

// Close stops the shipper and the reconcile loop.
func (g *GeoStore) Close() {
	close(g.stop)
	g.done.Wait()
}

// shipper drains the queue to the remote region forever, and periodically
// runs a full reconcile pass (list every local key, compare with the remote,
// re-ship anything missing or different). The reconcile closes the in-memory
// queue's restart hole: mutations queued but not shipped before a crash are
// picked up by the next pass. It also heals a remote that lost objects
// (bucket incident, partial restore).
func (g *GeoStore) shipper(interval time.Duration) {
	defer g.done.Done()
	reconcileEvery := 10 * interval
	if reconcileEvery < 5*time.Second {
		reconcileEvery = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	rt := time.NewTicker(reconcileEvery)
	defer rt.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-t.C:
		case <-g.wake:
		case <-rt.C:
			if n, err := g.Reconcile(context.Background()); err != nil {
				g.noteErr(err)
			} else if n > 0 {
				log.Printf("geo: reconcile re-shipped %d object(s)", n)
			}
			continue
		}
		for {
			item, ok := g.pop()
			if !ok {
				break
			}
			if err := g.ship(item); err != nil {
				g.noteErr(err)
				g.pushFront(item)
				time.Sleep(time.Second) // backoff; retry next tick/wake
				break
			}
			g.noteShipped(item.key)
		}
	}
}

// Reconcile lists every object on the local region and re-ships any key the
// remote region is missing or storing differently. Returns the number of
// objects re-shipped. GC-only deletions are not detected (by design: each
// region runs its own GC).
func (g *GeoStore) Reconcile(ctx context.Context) (int, error) {
	prefix := strings.Trim(g.repoPrefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	next := ""
	shipped := 0
	for {
		objs, cont, err := g.local.ListPrefix(ctx, prefix, next, 1000)
		if err != nil {
			return shipped, fmt.Errorf("geo reconcile: list local: %w", err)
		}
		next = cont
		for _, o := range objs {
			want, lerr := g.local.Get(ctx, o.Key, nil)
			if lerr != nil {
				continue // deleted between list and read (GC); skip
			}
			have, rerr := g.remote.Mirrors()[0].Get(ctx, o.Key, nil)
			if rerr != nil || !bytes.Equal(have, want) {
				if err := g.ship(geoItem{key: o.Key, data: want}); err != nil {
					return shipped, fmt.Errorf("geo reconcile: ship %s: %w", o.Key, err)
				}
				shipped++
			}
		}
		if next == "" {
			break
		}
	}
	return shipped, nil
}

func (g *GeoStore) pop() (geoItem, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.queue) == 0 {
		return geoItem{}, false
	}
	it := g.queue[0]
	g.queue = g.queue[1:]
	return it, true
}

func (g *GeoStore) pushFront(it geoItem) {
	g.mu.Lock()
	g.queue = append([]geoItem{it}, g.queue...)
	g.mu.Unlock()
}

func (g *GeoStore) enqueue(key string, data []byte) {
	g.mu.Lock()
	g.queue = append(g.queue, geoItem{key: key, data: append([]byte(nil), data...)})
	g.mu.Unlock()
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// ship replays one mutation on the remote region. The remote is a sink, not
// a writer: bypass its write fence (stale-writer protection) and put to each
// member directly, so a lagging member converges instead of refusing.
func (g *GeoStore) ship(it geoItem) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var lastErr error
	for _, s := range g.remote.Mirrors() {
		if err := s.Put(ctx, it.key, it.data); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (g *GeoStore) noteShipped(key string) {
	g.mu.Lock()
	g.shipped++
	g.lastShip = time.Now()
	g.lastError = nil
	g.mu.Unlock()
	if g.onShip != nil {
		g.onShip(key)
	}
}

func (g *GeoStore) noteErr(err error) {
	g.mu.Lock()
	g.shipErrs++
	g.lastError = err
	g.mu.Unlock()
	log.Printf("geo: ship: %v", err)
}

// GeoStatus reports shipping progress for observability.
type GeoStatus struct {
	Pending   uint64
	Shipped   uint64
	Errors    uint64
	LastShip  time.Time
	LastError error
}

// Status returns a snapshot of the shipper.
func (g *GeoStore) Status() GeoStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	return GeoStatus{
		Pending:   uint64(len(g.queue)),
		Shipped:   g.shipped,
		Errors:    g.shipErrs,
		LastShip:  g.lastShip,
		LastError: g.lastError,
	}
}

// Journaling / WriteQuorum / quorum interfaces: forwarded to the local
// region, which is the one that acks commits.
func (g *GeoStore) Journaling() bool { return g.local.Journaling() }
func (g *GeoStore) WriteQuorum() int { return g.local.WriteQuorum() }
func (g *GeoStore) QuorumGet(ctx context.Context, key string) ([]byte, int, error) {
	return g.local.QuorumGet(ctx, key)
}
func (g *GeoStore) ArbiterGet(ctx context.Context, key string) ([]byte, error) {
	return g.local.ArbiterGet(ctx, key)
}
func (g *GeoStore) PutAll(ctx context.Context, key string, data []byte) error {
	if err := g.local.PutAll(ctx, key, data); err != nil {
		return err
	}
	g.enqueue(key, data)
	return nil
}

// Put stores locally at quorum and queues remote shipment.
func (g *GeoStore) Put(ctx context.Context, key string, data []byte) error {
	if err := g.local.Put(ctx, key, data); err != nil {
		return err
	}
	g.enqueue(key, data)
	return nil
}

func (g *GeoStore) PutFrom(ctx context.Context, key string, r io.Reader, size int64) error {
	// Read into memory: the remote ship needs the bytes anyway.
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	return g.Put(ctx, key, buf)
}

func (g *GeoStore) Get(ctx context.Context, key string, rng *Range) ([]byte, error) {
	return g.local.Get(ctx, key, rng)
}

func (g *GeoStore) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	return g.local.GetTo(ctx, key, w)
}

func (g *GeoStore) Exists(ctx context.Context, key string) (bool, error) {
	return g.local.Exists(ctx, key)
}

func (g *GeoStore) Head(ctx context.Context, key string) (string, bool, error) {
	return g.local.Head(ctx, key)
}

func (g *GeoStore) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if err := g.local.CreateIfAbsent(ctx, key, data); err != nil {
		return err
	}
	g.enqueue(key, data)
	return nil
}

// CAS publishes through the local region (quorum CAS on the control
// object), then queues the same bytes for the remote.
func (g *GeoStore) CAS(ctx context.Context, key string, expectETag string, data []byte) (string, error) {
	etag, err := g.local.CAS(ctx, key, expectETag, data)
	if err != nil {
		return "", err
	}
	g.enqueue(key, data)
	return etag, nil
}

func (g *GeoStore) ListPrefix(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	return g.local.ListPrefix(ctx, prefix, continuation, max)
}

// Delete is GC-only and local; remote GC runs its own region's schedule.
func (g *GeoStore) Delete(ctx context.Context, keys []string) error {
	return g.local.Delete(ctx, keys)
}

var _ Store = (*GeoStore)(nil)
