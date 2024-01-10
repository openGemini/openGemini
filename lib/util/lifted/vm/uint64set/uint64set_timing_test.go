package uint64set

import (
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func BenchmarkSetAddWithAllocs(b *testing.B) {
	for _, itemsCount := range []uint64{1e3, 1e4, 1e5, 1e6, 1e7} {
		b.Run(fmt.Sprintf("items_%d", itemsCount), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := uint64(time.Now().UnixNano())
					end := start + itemsCount
					var s Set
					n := start
					for n < end {
						s.Add(n)
						n++
					}
				}
			})
		})
	}
}

func BenchmarkRoaringBitmapAddWithAllocs(b *testing.B) {
	for _, itemsCount := range []uint64{1e3, 1e4, 1e5, 1e6, 1e7} {
		b.Run(fmt.Sprintf("items_%d", itemsCount), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := uint64(time.Now().UnixNano())
					end := start + itemsCount
					var bitmap roaring64.Bitmap
					n := start
					for n < end {
						bitmap.Add(n)
						n++
					}
				}
			})
		})
	}
}

func BenchmarkSetIteratorWithAllocs(b *testing.B) {
	for _, itemsCount := range []uint64{1e3, 1e4, 1e5, 1e6, 1e7} {
		b.Run(fmt.Sprintf("items_%d", itemsCount), func(b *testing.B) {
			start := uint64(time.Now().UnixNano())
			end := start + itemsCount
			var s Set
			n := start
			for n < end {
				s.Add(n)
				n++
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					itr := s.Iterator()
					for itr.HasNext() {
						itr.Next()
					}
				}
			})
		})
	}
}

func BenchmarkBitmapIteratorWithAllocs(b *testing.B) {
	for _, itemsCount := range []uint64{1e3, 1e4, 1e5, 1e6, 1e7} {
		b.Run(fmt.Sprintf("items_%d", itemsCount), func(b *testing.B) {
			start := uint64(time.Now().UnixNano())
			end := start + itemsCount
			var bitmap roaring64.Bitmap
			n := start
			for n < end {
				bitmap.Add(n)
				n++
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					itr := bitmap.Iterator()
					for itr.HasNext() {
						itr.Next()
					}
				}
			})
		})
	}
}
