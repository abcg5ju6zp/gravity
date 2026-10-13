package types

const (
	EventTopicDNSRecordCreateForward = "roles.dns.record.create_forward"
	EventTopicDNSRecordCreateReverse = "roles.dns.record.create_reverse"
	// EventTopicDNSRecordEnsure supports the discovery apply dry-run/ensure
	// contract for a single forward or reverse record.
	EventTopicDNSRecordEnsure = "roles.dns.record.ensure"
)
