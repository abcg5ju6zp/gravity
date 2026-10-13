package tftp

import "time"

// Boot bundle version lifecycle states.
const (
	// bootStateCandidate: uploaded (fully or partially), not yet serving.
	bootStateCandidate = "candidate"
	// bootStatePublished: the current serving version of its bundle.
	bootStatePublished = "published"
	// bootStateSuperseded: was published once, replaced by a newer version.
	bootStateSuperseded = "superseded"
	// bootStateCancelled: explicitly cancelled candidate, kept for query until GC.
	bootStateCancelled = "cancelled"
)

// BootFileSpec declares one file of a boot bundle version manifest.
type BootFileSpec struct {
	// Path is the file's relative path inside the bundle (no leading slash).
	Path string `json:"path"`
	// Required files must be received before the version can be published.
	Required bool `json:"required"`
	// Entrypoint files pin a TFTP client's version on first read.
	Entrypoint bool `json:"entrypoint"`
	// SHA256 is an optional lowercase hex digest the uploaded content must match.
	SHA256 string `json:"sha256,omitempty"`
}

// BootReceivedFile records an accepted upload for a manifest path.
type BootReceivedFile struct {
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// BootVersion is the immutable, stable-identity version of a boot bundle.
// The manifest never changes after creation; file blobs under the version are
// write-once, and only the state/publishSeq/received fields transition.
type BootVersion struct {
	Bundle   string                      `json:"bundle"`
	ID       string                      `json:"id"`
	State    string                      `json:"state"`
	Files    []BootFileSpec              `json:"files"`
	Received map[string]BootReceivedFile `json:"received,omitempty"`
	// PublishSeq is the monotonic fencing sequence assigned at publish time.
	PublishSeq int64     `json:"publishSeq,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// BootBundle is the single publish point: CurrentVersionID points at the one
// published version served to unpinned clients.
type BootBundle struct {
	Name             string    `json:"name"`
	CurrentVersionID string    `json:"currentVersionId,omitempty"`
	PublishSeq       int64     `json:"publishSeq"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// Missing returns the relative paths of required manifest files that have not
// been accepted yet.
func (v *BootVersion) Missing() []string {
	missing := []string{}
	for _, f := range v.Files {
		if !f.Required {
			continue
		}
		if _, ok := v.Received[f.Path]; !ok {
			missing = append(missing, f.Path)
		}
	}
	return missing
}

// Complete reports whether every required file has been received.
func (v *BootVersion) Complete() bool {
	return len(v.Missing()) == 0
}

// specFor looks up the manifest entry for a path.
func (v *BootVersion) specFor(path string) (BootFileSpec, bool) {
	for _, f := range v.Files {
		if f.Path == path {
			return f, true
		}
	}
	return BootFileSpec{}, false
}
