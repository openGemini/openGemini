//go:build linux || freebsd
// +build linux freebsd

package fs

// EnableMmap set enableMmap
func EnableMmap(off bool) {
	enableMmap = off
}
