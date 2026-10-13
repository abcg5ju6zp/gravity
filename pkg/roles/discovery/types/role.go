package types

const (
	KeyRole = "discovery"

	KeySubnets = "subnets"
	KeyDevices = "devices"

	// KeyScans holds immutable scan metadata at {KeyScans}/{subnet}/{scanID}
	// and immutable device snapshots at {KeyScans}/{subnet}/{scanID}/devices/{identifier}.
	KeyScans = "scans"

	// KeyApplies holds apply jobs at {KeyApplies}/{applyID}.
	KeyApplies = "applies"
	// KeyAppliesIdem maps a client provided idempotency key to an applyID.
	KeyAppliesIdem = "applies-idem"
	// KeyApplyLocks is used for the per-job etcd concurrency mutex.
	KeyApplyLocks = "apply-locks"
)
