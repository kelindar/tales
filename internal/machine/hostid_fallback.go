// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Portions adapted from github.com/rs/xid.

//go:build !darwin && !linux && !freebsd && !windows

package machine

import "errors"

func readPlatformMachineID() (string, error) {
	return "", errors.New("machine: platform ID not implemented")
}
