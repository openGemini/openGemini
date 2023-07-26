/*
Copyright (c) 2013-2018 Influxdata Inc.
This code is originally from: https://github.com/influxdata/influxdb/blame/v2.7.0/internal/fs/influx_dir.go
*/

package config

import (
	"os"
	"os/user"
	"path/filepath"
)

// openGeminiDir retrieves the openGemini directory.
func openGeminiDir() string {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		dir = wd
	}
	homeDir := filepath.Join(dir, ".openGemini")

	return homeDir
}
