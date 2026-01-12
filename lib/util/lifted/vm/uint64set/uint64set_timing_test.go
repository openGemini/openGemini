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

func generateSet(n int) *Set {
	s := new(Set)
	start := uint64(time.Now().UnixNano())
	end := start + uint64(n)
	for start < end {
		s.Add(start)
		start++
	}
	return s
}

func BenchmarkUnion(b *testing.B) {
	/*
	   BenchmarkUnion/union
	   BenchmarkUnion/union-8         	   26275	     43284 ns/op	      96 B/op	       1 allocs/op
	   BenchmarkUnion/union#01
	   BenchmarkUnion/union#01-8      	   28395	     40106 ns/op	       0 B/op	       0 allocs/op
	*/
	a := generateSet(1000000)
	s := generateSet(1000000)

	b.Run("union", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.Union(a)
		}
	})

	b.Run("union", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.UnionMayOwn(a)
		}
	})
}
