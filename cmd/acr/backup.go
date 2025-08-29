// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
)

// backupTagsToACR backs up tags to another Azure Container Registry using BeginImportImage
func backupTagsToACR(ctx context.Context, sourceLoginURL string, tagsToBackup []acr.TagAttributesBase, backupRegistryName string, backupSubscriptionID string, backupResourceGroup string, sourceSubscriptionID string, sourceResourceGroup string, repoName string) error {
	if len(tagsToBackup) == 0 {
		return nil
	}

	fmt.Printf("Backing up %d tags from %s/%s to %s...\n", len(tagsToBackup), sourceLoginURL, repoName, backupRegistryName)

	// Create Azure credential
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to obtain Azure credential: %w", err)
	}

	// Create Azure Container Registry client
	clientFactory, err := armcontainerregistry.NewClientFactory(backupSubscriptionID, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create ACR client factory: %w", err)
	}

	registriesClient := clientFactory.NewRegistriesClient()

	// Extract source registry name from loginURL (remove .azurecr.io suffix)
	sourceRegistryName := strings.TrimSuffix(sourceLoginURL, ".azurecr.io")
	if strings.Contains(sourceRegistryName, "://") {
		// Handle URLs like https://myregistry.azurecr.io
		parts := strings.Split(sourceRegistryName, "://")
		if len(parts) > 1 {
			sourceRegistryName = strings.TrimSuffix(parts[1], ".azurecr.io")
		}
	}

	// Track backup success
	successCount := 0

	// Backup each tag
	for _, tag := range tagsToBackup {
		if tag.Name == nil {
			fmt.Printf("Warning: Skipping tag with nil name\n")
			continue
		}

		sourceImage := fmt.Sprintf("%s:%s", repoName, *tag.Name)
		targetTag := fmt.Sprintf("%s:%s", repoName, *tag.Name)

		fmt.Printf("Backing up tag: %s -> %s/%s\n", sourceImage, backupRegistryName, targetTag)

		// Construct source resource ID
		sourceResourceID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.ContainerRegistry/registries/%s",
			sourceSubscriptionID, sourceResourceGroup, sourceRegistryName)

		importParams := armcontainerregistry.ImportImageParameters{
			Mode: to.Ptr(armcontainerregistry.ImportModeForce),
			Source: &armcontainerregistry.ImportSource{
				ResourceID:  to.Ptr(sourceResourceID),
				SourceImage: to.Ptr(sourceImage),
			},
			TargetTags: []*string{
				to.Ptr(targetTag),
			},
		}

		// Start the import operation
		poller, err := registriesClient.BeginImportImage(ctx, backupResourceGroup, backupRegistryName, importParams, nil)
		if err != nil {
			return fmt.Errorf("failed to start backup for %s: %w", sourceImage, err)
		}

		// Wait for the import to complete
		_, err = poller.PollUntilDone(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to complete backup for %s: %w", sourceImage, err)
		}

		fmt.Printf("Successfully backed up: %s\n", sourceImage)
		successCount++
	}

	fmt.Printf("All %d tags backed up successfully\n", successCount)
	return nil
}
