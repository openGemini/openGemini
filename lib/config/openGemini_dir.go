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

// openGeminiDir returns the default directory for openGemini data and metadata storage.
// The directory is determined in the following priority order:
// 1. Current user's home directory (from user.Current())
// 2. HOME environment variable
// 3. Current working directory (as a fallback)
// The final path is the selected directory with ".openGemini" appended.
func openGeminiDir() string {
	baseDir := getBaseDirectory()
	return filepath.Join(baseDir, ".openGemini")
}

// getBaseDirectory determines the base directory to use for openGemini storage
// by checking user home, environment variables, and working directory in order.
func getBaseDirectory() string {
	// Try to get current user's home directory first
	if u, err := user.Current(); err == nil && u.HomeDir != "" {
		return u.HomeDir
	}

	// Fallback to HOME environment variable if user lookup failed
	if home := os.Getenv("HOME"); home != "" {
		return home
	}

	// Final fallback: use current working directory
	wd, err := os.Getwd()
	if err != nil {
		panic(err) // Unable to determine any valid directory
	}
	return wd
}
