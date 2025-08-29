// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"strings"
)

// isManifestNotFoundError checks if the error is related to manifest digest not being found
// This handles the specific case where backup fails due to manifest not existing in source registry
func isManifestNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	// Check for both the error code and the specific message
	return (strings.Contains(errStr, "invalidparameters") || strings.Contains(errStr, "400")) &&
		strings.Contains(errStr, "unable to find manifest digest")
}
