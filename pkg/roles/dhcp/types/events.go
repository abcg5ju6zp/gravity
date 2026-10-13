package types

const (
	EventTopicDHCPCreateLease = "roles.dhcp.lease.create"
	// EventTopicDHCPLeaseEnsure supports the discovery apply dry-run/ensure
	// contract: the handler either reports the current state (dry-run) or
	// performs a fenced compare-and-swap reservation write.
	EventTopicDHCPLeaseEnsure = "roles.dhcp.lease.ensure"
)

const (
	EventTopicDHCPLeasePut = "roles.dhcp.lease.put"
)
