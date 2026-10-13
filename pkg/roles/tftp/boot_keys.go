package tftp

import (
	"errors"
	"regexp"
	"strings"

	"beryju.io/gravity/pkg/roles/tftp/types"
	"beryju.io/gravity/pkg/storage"
)

// bootPrefix is the reserved TFTP filename prefix for bundle reads:
// boot/<bundle>/<path within the version>.
const bootPrefix = "boot/"

var (
	// bootNameRe covers bundle names and client-supplied version IDs.
	bootNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
	sha256Re   = regexp.MustCompile(`^[a-f0-9]{64}$`)

	errBootNameEmpty       = errors.New("boot bundle/version name must not be empty")
	errBootNameInvalid     = errors.New("boot bundle/version name contains invalid characters")
	errBootPathEmpty       = errors.New("boot file path must not be empty")
	errBootPathAbsolute    = errors.New("boot file path must be relative")
	errBootPathSegment     = errors.New("boot file path contains empty, '.' or '..' segments")
	errBootPathBackslash   = errors.New("boot file path must not contain backslashes")
	errBootPathReserved    = errors.New("boot file path must not use the reserved 'boot/' prefix")
	errBootManifestEmpty   = errors.New("boot version manifest must declare at least one file")
	errBootManifestDup     = errors.New("boot version manifest contains duplicate file paths")
	errBootManifestNoEntry = errors.New("boot version manifest must declare at least one required entrypoint file")
	errBootBadChecksum     = errors.New("boot file sha256 must be 64 lowercase hex characters")
)

func validateBootName(name string) error {
	if name == "" {
		return errBootNameEmpty
	}
	if !bootNameRe.MatchString(name) {
		return errBootNameInvalid
	}
	return nil
}

// validateBootRelativePath validates a bundle-inclusive relative file path.
func validateBootRelativePath(path string) error {
	if path == "" {
		return errBootPathEmpty
	}
	if strings.HasPrefix(path, "/") {
		return errBootPathAbsolute
	}
	if strings.ContainsRune(path, '\\') {
		return errBootPathBackslash
	}
	if strings.HasPrefix(path, bootPrefix) {
		return errBootPathReserved
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return errBootPathSegment
		}
	}
	return nil
}

// validateBootManifest validates a candidate version's file manifest.
func validateBootManifest(files []BootFileSpec) error {
	if len(files) == 0 {
		return errBootManifestEmpty
	}
	seen := map[string]struct{}{}
	entries := 0
	for _, f := range files {
		if err := validateBootRelativePath(f.Path); err != nil {
			return err
		}
		if _, dup := seen[f.Path]; dup {
			return errBootManifestDup
		}
		seen[f.Path] = struct{}{}
		if f.Entrypoint {
			if !f.Required {
				return errBootManifestNoEntry
			}
			entries++
		}
		if f.SHA256 != "" && !sha256Re.MatchString(f.SHA256) {
			return errBootBadChecksum
		}
	}
	if entries < 1 {
		return errBootManifestNoEntry
	}
	return nil
}

// --- etcd key layout (see constants in types/role.go) ---

func bootBundlesRootKey(kv *storage.Client) *storage.Key {
	return kv.Key(
		types.KeyRole,
		types.KeyBoot,
		types.KeyBootBundles,
	)
}

func bootBundleMetaKey(kv *storage.Client, bundle string) *storage.Key {
	return bootBundlesRootKey(kv).Add(bundle, types.KeyBootMeta)
}

func bootVersionsPrefixKey(kv *storage.Client, bundle string) *storage.Key {
	return bootBundlesRootKey(kv).Add(bundle, types.KeyBootVersions).Prefix(true)
}

func bootVersionMetaKey(kv *storage.Client, bundle, version string) *storage.Key {
	return bootBundlesRootKey(kv).Add(bundle, types.KeyBootVersions, version, types.KeyBootMeta)
}

func bootVersionFilesPrefixKey(kv *storage.Client, bundle, version string) *storage.Key {
	return bootBundlesRootKey(kv).Add(
		bundle, types.KeyBootVersions, version, types.KeyBootFiles,
	).Prefix(true)
}

func bootVersionFileKey(kv *storage.Client, bundle, version, path string) *storage.Key {
	return bootBundlesRootKey(kv).Add(
		bundle, types.KeyBootVersions, version, types.KeyBootFiles, path,
	)
}

// splitBootFilename splits a TFTP boot filename of the form
// boot/<bundle>/<path>; ok=false for any other shape.
func splitBootFilename(filename string) (bundle, path string, ok bool) {
	if !strings.HasPrefix(filename, bootPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(filename, bootPrefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	if err := validateBootName(parts[0]); err != nil {
		return "", "", false
	}
	if err := validateBootRelativePath(parts[1]); err != nil {
		return "", "", false
	}
	return parts[0], parts[1], true
}
