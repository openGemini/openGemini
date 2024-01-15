package influx

var unmarshalWorkPool chan *unmarshalWork

func GetUnmarshalWork() *unmarshalWork {
	select {
	case uw := <-unmarshalWorkPool:
		return uw
	default:
		return &unmarshalWork{}
	}
}

func putUnmarshalWork(uw *unmarshalWork) {
	uw.reset()
	select {
	case unmarshalWorkPool <- uw:
	default:
	}
}
