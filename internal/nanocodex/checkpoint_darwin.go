package nanocodex

import "syscall"

// statDevice returns the device number of a file's stat result. Darwin
// reports it as a signed 32-bit value; device numbers are non-negative.
func statDevice(system *syscall.Stat_t) uint64 {
	return uint64(system.Dev) //nolint:gosec // Device numbers are non-negative.
}
