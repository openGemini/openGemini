package meta

/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/v0.10.3/services/meta/store_fsm.go
*/

import (
	"github.com/hashicorp/raft"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
)

type storeFSMSnapshot struct {
	Data *meta2.Data
}

func (s *storeFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		p, err := s.Data.MarshalBinary()
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(p); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sinkErr := sink.Cancel()
		if sinkErr != nil {
			return sinkErr
		}
	}

	return nil
}

// Release is invoked when we are finished with the snapshot
func (s *storeFSMSnapshot) Release() {}
