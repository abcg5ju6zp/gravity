package discovery

import (
	"context"
	"testing"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/storage"
	"beryju.io/gravity/pkg/tests"
	"go.uber.org/zap"
)

type discoveryTestMigrator struct{}

func (discoveryTestMigrator) AddMigration(roles.Migration) {}

func (discoveryTestMigrator) Run(context.Context) (*storage.Client, error) {
	return extconfig.Get().EtcdClient(), nil
}

type discoveryTestInstance struct {
	ctx context.Context
	log *zap.Logger
	kv  *storage.Client
}

func newDiscoveryTestInstance() *discoveryTestInstance {
	return &discoveryTestInstance{
		ctx: tests.Context(),
		log: extconfig.Get().Logger().Named("role.discovery.test"),
		kv:  extconfig.Get().EtcdClient(),
	}
}

func (i *discoveryTestInstance) KV() *storage.Client                         { return i.kv }
func (i *discoveryTestInstance) Log() *zap.Logger                            { return i.log }
func (i *discoveryTestInstance) DispatchEvent(string, *roles.Event)          {}
func (i *discoveryTestInstance) AddEventListener(string, roles.EventHandler) {}
func (i *discoveryTestInstance) Context() context.Context                    { return i.ctx }
func (i *discoveryTestInstance) ExecuteHook(roles.HookOptions, ...interface{}) interface{} {
	return nil
}
func (i *discoveryTestInstance) Migrator() roles.RoleMigrator {
	return discoveryTestMigrator{}
}

func newTestRole(t *testing.T) *Role {
	t.Helper()
	return New(newDiscoveryTestInstance())
}

func testSubnet(r *Role, ttl int) *Subnet {
	s := r.NewSubnet("test-subnet")
	s.CIDR = "192.0.2.0/24"
	s.DNSResolver = "1.1.1.1:53"
	s.DiscoveryTTL = ttl
	return s
}
