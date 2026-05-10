//go:build plugin
// +build plugin

// Plugin entry point for building influxdb as a .so dynamic plugin.
// Build with: go build -tags=plugin -buildmode=plugin -o influxdb.so .
package main

import (
	// Trigger init() registration in the influxdb plugin package.
	_ "github.com/silves-xiang/data-bridge/plugins/influxdb"
)

// Register is the exported symbol called by the databridge dynamic plugin loader.
// All source/sink/hook registrations happen via init() in the imported packages.
func Register() {
}
