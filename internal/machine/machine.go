// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package machine

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

var (
	once sync.Once
	host uint32
	pid  = processID()
)

func processID() int {
	id := os.Getpid()
	if cpuset, err := os.ReadFile("/proc/self/cpuset"); err == nil && len(cpuset) > 1 {
		id ^= int(crc32.ChecksumIEEE(cpuset))
	}
	return id
}

// ID returns a process replica ID packed as machine bits followed by PID bits.
func ID() uint64 {
	once.Do(func() { host = readMachineID() })
	return uint64(host&0x7fffffff)<<32 | uint64(uint32(pid))
}

func readMachineID() uint32 {
	var id [4]byte
	value, err := readPlatformMachineID()
	if err != nil || value == "" {
		value, err = os.Hostname()
	}
	if err == nil && value != "" {
		sum := sha256.Sum256([]byte(value))
		copy(id[:], sum[:])
	} else if _, err := rand.Read(id[:]); err != nil {
		return uint32(os.Getpid()) & 0x7fffffff
	}
	return binary.BigEndian.Uint32(id[:]) & 0x7fffffff
}
