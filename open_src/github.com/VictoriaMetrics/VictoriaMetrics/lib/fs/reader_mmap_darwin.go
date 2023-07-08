//go:build darwin
// +build darwin

package fs

// EnableMmap disable mmap on darwin always.
func EnableMmap(off bool) {}
