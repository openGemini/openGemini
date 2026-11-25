package workingsetcache

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
)

type Cache = workingsetcache.Cache

var Load = workingsetcache.Load
var New = workingsetcache.New

// Save preserves the index shutdown API's error result for cache write failures.
func Save(cache *Cache, path string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			saveErr, ok := r.(error)
			if !ok || !strings.HasPrefix(saveErr.Error(), "FATAL: cannot save cache to ") {
				panic(r)
			}
			err = saveErr
		}
	}()
	cache.MustSave(path)
	return nil
}
