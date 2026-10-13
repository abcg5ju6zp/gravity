package types

const (
	KeyRole  = "tftp"
	KeyFiles = "files"

	// Boot bundle (immutable, versioned network boot material) key hierarchy:
	// /tftp/boot/bundles/<bundle>/meta
	// /tftp/boot/bundles/<bundle>/versions/<version>/meta
	// /tftp/boot/bundles/<bundle>/versions/<version>/files/<path>
	KeyBoot         = "boot"
	KeyBootBundles  = "bundles"
	KeyBootVersions = "versions"
	KeyBootFiles    = "files"
	KeyBootMeta     = "meta"
)
