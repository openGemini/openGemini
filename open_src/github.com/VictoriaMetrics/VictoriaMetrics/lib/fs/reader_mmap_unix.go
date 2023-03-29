//go:build linux || freebsd
// +build linux freebsd

package fs

// SetDisableMmap set disableMmap
func SetDisableMmap(off bool) {
	disableMmap = off
}
