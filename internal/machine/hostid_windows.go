// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Portions adapted from github.com/rs/xid.

//go:build windows

package machine

import (
	"fmt"
	"syscall"
	"unsafe"
)

func readPlatformMachineID() (string, error) {
	key, err := syscall.UTF16PtrFromString(`SOFTWARE\Microsoft\Cryptography`)
	if err != nil {
		return "", fmt.Errorf("machine: read registry key: %w", err)
	}

	var handle syscall.Handle
	if err := syscall.RegOpenKeyEx(syscall.HKEY_LOCAL_MACHINE, key, 0, syscall.KEY_READ|syscall.KEY_WOW64_64KEY, &handle); err != nil {
		return "", err
	}
	defer func() { _ = syscall.RegCloseKey(handle) }()

	name, err := syscall.UTF16PtrFromString("MachineGuid")
	if err != nil {
		return "", fmt.Errorf("machine: read machine GUID: %w", err)
	}
	var value [74]uint16
	size := uint32(len(value))
	var valueType uint32
	if err := syscall.RegQueryValueEx(handle, name, nil, &valueType, (*byte)(unsafe.Pointer(&value[0])), &size); err != nil {
		return "", err
	}
	id := syscall.UTF16ToString(value[:])
	if len(id) != 36 {
		return "", fmt.Errorf("machine: invalid host ID %q", id)
	}
	return id, nil
}
