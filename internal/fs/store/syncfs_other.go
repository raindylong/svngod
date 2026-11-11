//go:build !linux

package store

// syncFs has no portable filesystem-wide flush outside Linux. Per-write
// durability is already ensured by the file+directory fsyncs in the
// syncWrites path, so on other platforms SyncAll is only a no-op bulk
// fast-path that we skip. handled=false tells the caller to ignore the result.
func syncFs(_ int) (handled bool, err error) {
	return false, nil
}
