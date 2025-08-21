/*
Copyright (c) 2013-2016 Errplane Inc.
This code is originally from: https://github.com/influxdata/influxdb/blob/v0.10.3/services/meta/store_fsm.go
*/

package meta

import (
	"github.com/hashicorp/raft"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

// storeFSMSnapshot holds a snapshot of the meta store's data for Raft
type storeFSMSnapshot struct {
	Data *meta2.Data // The snapshot data of the meta store
}

// Persist writes the snapshot data to the provided sink
// Implements raft.FSMSnapshot interface
func (s *storeFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// Encapsulate persistence logic in a function for clear error handling
	persist := func() error {
		// Serialize the meta data to binary format
		dataBytes, err := s.Data.MarshalBinary()
		if err != nil {
			return err
		}

		// Write serialized data to the snapshot sink
		if _, err := sink.Write(dataBytes); err != nil {
			return err
		}

		// Close the sink to complete the snapshot
		return sink.Close()
	}

	// Execute persistence and handle errors
	if err := persist(); err != nil {
		// Attempt to cancel the sink if persistence fails
		if cancelErr := sink.Cancel(); cancelErr != nil {
			return cancelErr
		}
		return err
	}

	return nil
}

// Release is called when the snapshot is no longer needed
// Implements raft.FSMSnapshot interface
func (s *storeFSMSnapshot) Release() {}
