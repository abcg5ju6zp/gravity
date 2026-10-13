package roles

// Ensure action constants reported by role handlers that support the
// discovery apply dry-run/ensure event contract.
const (
	EnsureActionCreate   = "create"
	EnsureActionKeep     = "keep"
	EnsureActionConflict = "conflict"
	EnsureActionSkip     = "skip"
)

// EnsureOutcome is reported by a role handler (DHCP/DNS) for a single
// preview (dry-run) or ensure (conditional write) step. Discovery applies
// use the outcomes to pin target revisions during preview and to record
// per-step confirmation without knowing the target role's key layout.
type EnsureOutcome struct {
	// Step is the well-known step name from discovery/types this outcome
	// belongs to (e.g. dhcp_reservation, dns_forward, dns_reverse).
	Step string `json:"step"`
	// TargetKey is the absolute etcd key of the object that would be or
	// was written.
	TargetKey string `json:"targetKey"`
	// Current revisions of the target key at reporting time (0 when the
	// key does not exist).
	CreateRevision int64 `json:"createRevision"`
	ModRevision    int64 `json:"modRevision"`
	// Action is one of the EnsureAction* constants.
	Action string `json:"action"`
	// Reason carries a stable reason code for conflict/skip outcomes.
	Reason string `json:"reason,omitempty"`
	// AlreadyProvisioned is true when an existing object already carries
	// the requesting apply identity - retries reconcile instead of writing.
	AlreadyProvisioned bool `json:"alreadyProvisioned,omitempty"`
	// Siblings is the number of other records (different uid) at the same
	// name/type. Only relevant for DNS records, which are multi-valued.
	Siblings int64 `json:"siblings,omitempty"`
	// ErrorMessage carries a non-conflict, infrastructure level error.
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// ReportOutcome attaches an ensure outcome to the event. It is safe to call
// from multiple handlers; DispatchEvent invokes handlers synchronously but
// handlers may fan out internally.
func (ev *Event) ReportOutcome(outcome EnsureOutcome) {
	ev.Payload.outcomesMu.Lock()
	defer ev.Payload.outcomesMu.Unlock()
	ev.Payload.outcomes = append(ev.Payload.outcomes, outcome)
}

// Outcomes returns a copy of all ensure outcomes reported for this event.
func (ev *Event) Outcomes() []EnsureOutcome {
	ev.Payload.outcomesMu.Lock()
	defer ev.Payload.outcomesMu.Unlock()
	out := make([]EnsureOutcome, len(ev.Payload.outcomes))
	copy(out, ev.Payload.outcomes)
	return out
}
