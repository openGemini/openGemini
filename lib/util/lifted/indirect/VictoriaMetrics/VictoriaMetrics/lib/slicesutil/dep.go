package slicesutil

func SetLength[T any](a []T, newLen int) []T {
	if n := newLen - cap(a); n > 0 {
		a = append(a[:cap(a)], make([]T, n)...)
	}
	return a[:newLen]
}
