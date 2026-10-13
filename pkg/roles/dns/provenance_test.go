package dns_test

import (
	"encoding/json"
	"testing"

	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/roles/dns"
	"github.com/stretchr/testify/assert"
)

func TestRecordProvenanceJSONCompat(t *testing.T) {
	// Old-style record JSON (no discovery field) must round-trip unchanged.
	oldJSON := `{"data":"192.0.2.1"}`
	var rec dns.Record
	assert.NoError(t, json.Unmarshal([]byte(oldJSON), &rec))
	raw, err := json.Marshal(&rec)
	assert.NoError(t, err)
	assert.NotContains(t, string(raw), "discovery")

	// New-style record JSON carries the provenance.
	var rec2 dns.Record
	assert.NoError(t, json.Unmarshal([]byte(oldJSON), &rec2))
	// Discovery is an exported pointer field so external packages can set it
	// via JSON; construct the wire form directly to assert compatibility.
	withProv := `{"data":"192.0.2.1","discovery":{"applyId":"apply-9","scanId":"scan-9","source":"` + discoveryTypes.ProvenanceSource + `"}}`
	var rec3 dns.Record
	assert.NoError(t, json.Unmarshal([]byte(withProv), &rec3))
	raw3, err := json.Marshal(&rec3)
	assert.NoError(t, err)
	assert.Contains(t, string(raw3), `"applyId":"apply-9"`)
	assert.Contains(t, string(raw3), discoveryTypes.ProvenanceSource)
}
