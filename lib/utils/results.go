package utils

type SeriesId struct {
	Id   uint64
	Time int64
}

func NewSeriesId(id uint64, t int64) SeriesId {
	return SeriesId{
		Id:   id,
		Time: t,
	}
}
