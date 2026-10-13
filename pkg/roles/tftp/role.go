package tftp

import (
	"context"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles"
	apiTypes "beryju.io/gravity/pkg/roles/api/types"
	"beryju.io/gravity/pkg/roles/tftp/types"
	"beryju.io/gravity/pkg/storage"
	"github.com/pin/tftp/v3"
	"github.com/swaggest/rest/web"
	"go.uber.org/zap"
)

const etcdMaxSize = 10 * 1024 * 1024
const sharedNamespace = "shared"

type Role struct {
	localfs fs.FS
	servers []*tftp.Server

	boot              *bootManager
	bootJanitorCancel context.CancelFunc

	log *zap.Logger
	i   roles.Instance
	ctx context.Context
	cfg *RoleConfig
}

func init() {
	roles.Register("tftp", func(i roles.Instance) roles.Role {
		return New(i)
	})
}

func New(instance roles.Instance) *Role {
	r := &Role{
		log:     instance.Log(),
		i:       instance,
		ctx:     instance.Context(),
		localfs: os.DirFS(extconfig.Get().Dirs().TFTPLocalDir),
	}
	r.boot = newBootManager(r.i.KV(), r.log)
	r.i.AddEventListener(apiTypes.EventTopicAPIMuxSetup, func(ev *roles.Event) {
		svc := ev.Payload.Data["svc"].(*web.Service)
		svc.Get("/api/v1/tftp/files", r.APIFilesGet())
		svc.Get("/api/v1/tftp/files/download", r.APIFilesDownload())
		svc.Post("/api/v1/tftp/files", r.APIFilesPut())
		svc.Delete("/api/v1/tftp/files", r.APIFilesDelete())
		svc.Get("/api/v1/roles/tftp", r.APIRoleConfigGet())
		svc.Post("/api/v1/roles/tftp", r.APIRoleConfigPut())

		// Boot bundle APIs (immutable, versioned network boot material).
		svc.Get("/api/v1/tftp/boot/bundles", r.APIBootBundlesList())
		svc.Put("/api/v1/tftp/boot/bundles", r.APIBootBundleCreate())
		svc.Get("/api/v1/tftp/boot/versions", r.APIBootVersionsList())
		svc.Get("/api/v1/tftp/boot/versions/status", r.APIBootVersionStatus())
		svc.Get("/api/v1/tftp/boot/versions/download", r.APIBootVersionFileDownload())
		svc.Post("/api/v1/tftp/boot/versions", r.APIBootVersionCreate())
		svc.Put("/api/v1/tftp/boot/versions/file", r.APIBootVersionFilePut())
		svc.Post("/api/v1/tftp/boot/versions/publish", r.APIBootVersionPublish())
		svc.Post("/api/v1/tftp/boot/versions/cancel", r.APIBootVersionCancel())
		svc.Post("/api/v1/tftp/boot/gc", r.APIBootGC())
	})
	return r
}

func (r *Role) Start(ctx context.Context, config []byte) error {
	r.cfg = r.decodeRoleConfig(config)

	if err := r.boot.recoverState(ctx); err != nil {
		r.log.Warn("failed to recover boot bundle state", zap.Error(err))
	}
	if deleted, err := r.boot.garbageCollectStartup(ctx); err != nil {
		r.log.Warn("boot bundle startup garbage collection failed", zap.Error(err))
	} else if len(deleted) > 0 {
		r.log.Info("boot bundle startup garbage collection removed unreferenced versions",
			zap.Int("count", len(deleted)))
	}

	janitorCtx, cancel := context.WithCancel(ctx)
	r.bootJanitorCancel = cancel
	go r.boot.runJanitor(janitorCtx)

	for _, addr := range extconfig.Get().ListenAddrs(r.cfg.Port) {
		s := tftp.NewServer(r.Reader, r.Writer)
		s.SetHook(r)
		s.SetTimeout(5 * time.Second)
		r.servers = append(r.servers, s)
		r.log.Info("starting tftp server", zap.String("listen", addr))
		go func(s *tftp.Server, addr string) {
			if err := s.ListenAndServe(addr); err != nil && err != http.ErrServerClosed {
				r.log.Warn("failed to listen", zap.String("listen", addr), zap.Error(err))
			}
		}(s, addr)
	}
	return nil
}

func (r *Role) Stop() {
	if r.bootJanitorCancel != nil {
		r.bootJanitorCancel()
	}
	for _, s := range r.servers {
		s.Shutdown()
	}
}

func (r *Role) getPath(filename string, addr net.UDPAddr) *storage.Key {
	bk := r.i.KV().Key(
		types.KeyRole,
		types.KeyFiles,
	)
	if strings.HasPrefix(filename, sharedNamespace) {
		bk.Add(filename)
	} else {
		bk.Add(addr.IP.String(), filename)
	}
	return bk
}
