// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root
//
// Portions adapted from github.com/rs/xid.

//go:build darwin

package machine

import (
	"fmt"
	"os/exec"
	"strings"
)

func readPlatformMachineID() (string, error) {
	path, err := exec.LookPath("ioreg")
	if err != nil {
		return "", err
	}
	output, err := exec.Command(path, "-rd1", "-c", "IOPlatformExpertDevice").CombinedOutput()
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if parts := strings.SplitAfter(line, `" = "`); strings.Contains(line, "IOPlatformUUID") && len(parts) == 2 {
			return strings.ToLower(strings.TrimRight(parts[1], `"`)), nil
		}
	}
	return "", fmt.Errorf("platform UUID not found")
}
