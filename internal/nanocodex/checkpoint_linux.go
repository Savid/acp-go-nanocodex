package nanocodex

import "syscall"

// statDevice returns the device number of a file's stat result.
func statDevice(system *syscall.Stat_t) uint64 {
	return system.Dev
}
