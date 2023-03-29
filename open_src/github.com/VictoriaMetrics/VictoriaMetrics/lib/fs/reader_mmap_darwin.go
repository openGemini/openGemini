//go:build darwin
// +build darwin

package fs

// SetDisableMmap disable mmap on darwin always.
func SetDisableMmap(off bool) {}
