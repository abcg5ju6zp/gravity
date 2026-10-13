package types

// Provenance is recorded on every object created by a discovery apply
// job (the live device, the DHCP reservation and the forward/reverse DNS
// records). It links all objects of one application back to the same apply
// identity and the immutable scan result they originated from.
//
// All fields are optional in JSON so objects created outside of discovery
// applies (manually or via the legacy event chain) keep their old shape.
type Provenance struct {
	ApplyID string `json:"applyId"`
	ScanID  string `json:"scanId,omitempty"`
	Source  string `json:"source,omitempty"`
}

// ProvenanceSource is the fixed value stored in Provenance.Source for
// objects provisioned by a discovery apply job.
const ProvenanceSource = "discovery-apply"

// Scan lifecycle statuses.
const (
	ScanStatusRunning  = "running"
	ScanStatusFinished = "finished"
	ScanStatusFailed   = "failed"
)

// Apply job statuses.
const (
	JobStatusPending    = "pending"
	JobStatusRunning    = "running"
	JobStatusCompleted  = "completed"
	JobStatusPartial    = "partial"
	JobStatusFailed     = "failed"
	JobStatusCancelling = "cancelling"
	JobStatusCancelled  = "cancelled"
)

// Per-item results.
const (
	ItemResultPending   = "pending"
	ItemResultApplied   = "applied"
	ItemResultConflict  = "conflict"
	ItemResultFailed    = "failed"
	ItemResultCancelled = "cancelled"
	ItemResultSkipped   = "skipped"
)

// Per-step statuses.
const (
	StepStatusPending   = "pending"
	StepStatusConfirmed = "confirmed"
	StepStatusSkipped   = "skipped"
	StepStatusConflict  = "conflict"
	StepStatusFailed    = "failed"
	StepStatusCancelled = "cancelled"
)

// Well-known step names.
const (
	StepNameDeviceMark      = "device_mark"
	StepNameDHCPReservation = "dhcp_reservation"
	StepNameDNSForward      = "dns_forward"
	StepNameDNSReverse      = "dns_reverse"
)

// Apply targets.
const (
	TargetDHCP = "dhcp"
	TargetDNS  = "dns"
)

// Device sources.
const (
	DeviceSourceScan   = "scan"
	DeviceSourceManual = "manual"
)

// Event payload keys shared across discovery, dhcp and dns ensure events.
const (
	EventFieldDryRun            = "dryRun"
	EventFieldApplyID           = "applyId"
	EventFieldScanID            = "scanId"
	EventFieldFenceKey          = "fenceKey"
	EventFieldFenceRev          = "fenceRev"
	EventFieldExpectedCreateRev = "expectedCreateRev"
	EventFieldExpectedModRev    = "expectedModRev"
	EventFieldMAC               = "mac"
	EventFieldHostname          = "hostname"
	EventFieldAddress           = "address"
	EventFieldScope             = "scope"
	EventFieldFQDN              = "fqdn"
	EventFieldIdentifier        = "identifier"
	EventFieldDirection         = "direction"
	EventFieldTTL               = "ttl"
)

// DNS ensure directions.
const (
	EventDirectionForward = "forward"
	EventDirectionReverse = "reverse"
)

// Stable conflict / skip reason codes. They are part of the API contract
// and documented in the discovery role documentation.
const (
	ReasonScanNotFinished    = "scan_not_finished"
	ReasonScanExpired        = "scan_expired"
	ReasonSnapshotMismatch   = "snapshot_mismatch"
	ReasonDeviceMissing      = "device_missing"
	ReasonDeviceManual       = "device_changed_manual"
	ReasonDeviceNewerScan    = "device_changed_scan"
	ReasonDeviceClaimed      = "device_claimed"
	ReasonTargetChanged      = "target_changed"
	ReasonTargetOwnedOther   = "target_owned_other"
	ReasonScopeMissing       = "scope_missing"
	ReasonZoneMissing        = "zone_missing"
	ReasonRoleUnavailable    = "role_unavailable"
	ReasonMACRequired        = "mac_required"
	ReasonJobNotClaimable    = "job_not_claimable"
	ReasonAlreadyProvisioned = "already_provisioned"
)
