// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/acr-cli/internal/api"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/alitto/pond/v2"
)

// Purger purges tags or manifests concurrently.
type Purger struct {
	Executer
	acrClient            api.AcrCLIClientInterface
	includeLocked        bool
	enableBackup         bool
	backupRegistryName   string
	backupSubscriptionID string
	backupResourceGroup  string
	sourceSubscriptionID string
	sourceResourceGroup  string
	// Azure client components for backup (initialized once)
	azureCredential  *azidentity.DefaultAzureCredential
	registriesClient *armcontainerregistry.RegistriesClient
}

// NewPurger creates a new Purger. Purgers are currently repository specific
func NewPurger(repoParallelism int, acrClient api.AcrCLIClientInterface, loginURL string, repoName string, includeLocked bool, enableBackup bool, backupRegistryName string, backupSubscriptionID string, backupResourceGroup string, sourceSubscriptionID string, sourceResourceGroup string) *Purger {
	executeBase := Executer{
		// Use a queue size 3x the pool size to buffer enough tasks and keep workers busy and avoiding
		// slowdown due to task scheduling blocking.
		pool:     pond.NewPool(repoParallelism, pond.WithQueueSize(repoParallelism*3), pond.WithNonBlocking(false)),
		loginURL: loginURL,
		repoName: repoName,
	}

	purger := &Purger{
		Executer:             executeBase,
		acrClient:            acrClient,
		includeLocked:        includeLocked,
		enableBackup:         enableBackup,
		backupRegistryName:   backupRegistryName,
		backupSubscriptionID: backupSubscriptionID,
		backupResourceGroup:  backupResourceGroup,
		sourceSubscriptionID: sourceSubscriptionID,
		sourceResourceGroup:  sourceResourceGroup,
	}

	// Initialize Azure clients once if backup is enabled
	if enableBackup {
		// Create Azure credential
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			// Log the error but don't fail initialization - we'll handle this in backup operations
			fmt.Printf("Warning: Failed to initialize Azure credential for backup: %v\n", err)
		} else {
			purger.azureCredential = cred

			// Create Azure Container Registry client
			clientFactory, err := armcontainerregistry.NewClientFactory(backupSubscriptionID, cred, nil)
			if err != nil {
				fmt.Printf("Warning: Failed to create ACR client factory for backup: %v\n", err)
			} else {
				purger.registriesClient = clientFactory.NewRegistriesClient()
			}
		}
	}

	return purger
}

// backupSingleTag backs up a single tag to another Azure Container Registry using BeginImportImage
func (p *Purger) backupSingleTag(ctx context.Context, tag acr.TagAttributesBase) error {
	if !p.enableBackup {
		return nil
	}

	if tag.Name == nil {
		return fmt.Errorf("tag name is nil")
	}

	// Check if Azure clients are initialized
	if p.registriesClient == nil {
		return fmt.Errorf("Azure registry client not initialized for backup")
	}

	sourceImage := fmt.Sprintf("%s:%s", p.repoName, *tag.Name)
	targetTag := fmt.Sprintf("%s:%s", p.repoName, *tag.Name)

	fmt.Printf("Backing up tag: %s -> %s.azurecr.io/%s\n", sourceImage, p.backupRegistryName, targetTag)

	// Extract source registry name from loginURL (remove .azurecr.io suffix)
	sourceRegistryName := strings.TrimSuffix(p.loginURL, ".azurecr.io")
	if strings.Contains(sourceRegistryName, "://") {
		// Handle URLs like https://myregistry.azurecr.io
		parts := strings.Split(sourceRegistryName, "://")
		if len(parts) > 1 {
			sourceRegistryName = strings.TrimSuffix(parts[1], ".azurecr.io")
		}
	}

	// Construct source resource ID
	sourceResourceID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.ContainerRegistry/registries/%s",
		p.sourceSubscriptionID, p.sourceResourceGroup, sourceRegistryName)

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
	poller, err := p.registriesClient.BeginImportImage(ctx, p.backupResourceGroup, p.backupRegistryName, importParams, nil)
	if err != nil {
		return fmt.Errorf("failed to start backup for %s: %w", sourceImage, err)
	}

	// Wait for the import to complete
	_, err = poller.PollUntilDone(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to complete backup for %s: %w", sourceImage, err)
	}

	fmt.Printf("Successfully backed up: %s\n", sourceImage)
	return nil
}

// PurgeTags purges a list of tags concurrently, and returns a count of deleted tags and the first error occurred.
func (p *Purger) PurgeTags(ctx context.Context, tags []acr.TagAttributesBase) (int, error) {
	var deletedTags atomic.Int64 // Count of successfully deleted tags
	group := p.pool.NewGroup()
	for _, tag := range tags {
		group.SubmitErr(func() error {
			// Backup the tag first if backup is enabled
			if p.enableBackup {
				backupErr := p.backupSingleTag(ctx, tag)
				if backupErr != nil {
					fmt.Printf("Error: Failed to backup %s/%s:%s, error: %v. Skipping deletion due to backup failure.\n", p.loginURL, p.repoName, *tag.Name, backupErr)
					return backupErr
				}
			}

			// If include-locked is enabled and tag is locked, unlock it first
			if p.includeLocked && tag.ChangeableAttributes != nil {
				if (tag.ChangeableAttributes.DeleteEnabled != nil && !*tag.ChangeableAttributes.DeleteEnabled) ||
					(tag.ChangeableAttributes.WriteEnabled != nil && !*tag.ChangeableAttributes.WriteEnabled) {

					enabledTrue := true
					unlockAttrs := &acr.ChangeableAttributes{
						DeleteEnabled: &enabledTrue,
						WriteEnabled:  &enabledTrue,
					}

					_, unlockErr := p.acrClient.UpdateAcrTagAttributes(ctx, p.repoName, *tag.Name, unlockAttrs)
					if unlockErr != nil {
						fmt.Printf("Warning: Failed to unlock %s/%s:%s, error: %v. Will attempt deletion anyway.\n", p.loginURL, p.repoName, *tag.Name, unlockErr)
						// Continue to attempt deletion even if unlock fails
					} else {
						fmt.Printf("Unlocked %s/%s:%s\n", p.loginURL, p.repoName, *tag.Name)
					}
				}
			}

			resp, err := p.acrClient.DeleteAcrTag(ctx, p.repoName, *tag.Name)
			if err == nil {
				fmt.Printf("Deleted %s/%s:%s\n", p.loginURL, p.repoName, *tag.Name)
				// Increment the count of successfully deleted tags atomically
				deletedTags.Add(1)
				return nil
			}

			if resp != nil && resp.Response != nil {
				switch resp.StatusCode {
				case http.StatusNotFound:
					// If the tag is not found it can be assumed to have been deleted.
					deletedTags.Add(1)
					fmt.Printf("Skipped %s/%s:%s, HTTP status: %d\n", p.loginURL, p.repoName, *tag.Name, resp.StatusCode)
					return nil
				case http.StatusMethodNotAllowed:
					// Method not allowed - tag may be locked or operation not permitted
					fmt.Printf("Skipped %s/%s:%s, operation not allowed, HTTP status: %d\n", p.loginURL, p.repoName, *tag.Name, resp.StatusCode)
					return nil
				}
			}

			fmt.Printf("Failed to delete %s/%s:%s, error: %v\n", p.loginURL, p.repoName, *tag.Name, err)
			return err
		})
	}
	err := group.Wait() // Error should be nil
	return int(deletedTags.Load()), err
}

// PurgeManifests purges a list of manifests concurrently, and returns a count of deleted manifests and the first error occurred.
func (p *Purger) PurgeManifests(ctx context.Context, manifests []acr.ManifestAttributesBase) (int, error) {
	var deletedManifests atomic.Int64 // Count of successfully deleted tags
	group := p.pool.NewGroup()
	for _, manifest := range manifests {
		group.SubmitErr(func() error {
			// If include-locked is enabled and manifest is locked, unlock it first
			if p.includeLocked && manifest.ChangeableAttributes != nil {
				if (manifest.ChangeableAttributes.DeleteEnabled != nil && !*manifest.ChangeableAttributes.DeleteEnabled) ||
					(manifest.ChangeableAttributes.WriteEnabled != nil && !*manifest.ChangeableAttributes.WriteEnabled) {

					enabledTrue := true
					unlockAttrs := &acr.ChangeableAttributes{
						DeleteEnabled: &enabledTrue,
						WriteEnabled:  &enabledTrue,
					}

					_, unlockErr := p.acrClient.UpdateAcrManifestAttributes(ctx, p.repoName, *manifest.Digest, unlockAttrs)
					if unlockErr != nil {
						fmt.Printf("Warning: Failed to unlock %s/%s@%s, error: %v. Will attempt deletion anyway.\n", p.loginURL, p.repoName, *manifest.Digest, unlockErr)
						// Continue to attempt deletion even if unlock fails
					} else {
						fmt.Printf("Unlocked %s/%s@%s\n", p.loginURL, p.repoName, *manifest.Digest)
					}
				}
			}

			resp, err := p.acrClient.DeleteManifest(ctx, p.repoName, *manifest.Digest)
			if err == nil {
				fmt.Printf("Deleted %s/%s@%s\n", p.loginURL, p.repoName, *manifest.Digest)
				// Increment the count of successfully deleted tags atomically
				deletedManifests.Add(1)
				return nil
			}

			if resp != nil && resp.Response != nil {
				switch resp.StatusCode {
				case http.StatusNotFound:
					// If the manifest is not found it can be assumed to have been deleted.
					deletedManifests.Add(1)
					fmt.Printf("Skipped %s/%s@%s, HTTP status: %d\n", p.loginURL, p.repoName, *manifest.Digest, resp.StatusCode)
					return nil
				case http.StatusMethodNotAllowed:
					// Method not allowed - manifest may be locked or operation not permitted
					fmt.Printf("Skipped %s/%s@%s, operation not allowed, HTTP status: %d\n", p.loginURL, p.repoName, *manifest.Digest, resp.StatusCode)
					return nil
				}
			}

			fmt.Printf("Failed to delete %s/%s@%s, error: %v\n", p.loginURL, p.repoName, *manifest.Digest, err)
			return err

		})
	}
	err := group.Wait()
	return int(deletedManifests.Load()), err
}
