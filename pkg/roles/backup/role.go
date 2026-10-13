package backup

import (
	"context"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/api/types"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/robfig/cron/v3"
	"github.com/swaggest/rest/web"
	etcdsnapshot "go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"
)

const (
	EventTopicBackupRun = "roles.backup.run"
)

type Role struct {
	mc  *minio.Client
	cfg *RoleConfig
	c   *cron.Cron

	// version is captured once because extconfig.FullVersion mutates
	// package level variables and must not be called concurrently.
	version string

	jobsM sync.Mutex
	jobs  map[string]*activeJob
	snap  etcdsnapshot.Manager

	log *zap.Logger
	i   roles.Instance
	ctx context.Context
}

func init() {
	roles.Register("backup", func(i roles.Instance) roles.Role {
		return New(i)
	})
}

func New(instance roles.Instance) *Role {
	r := &Role{
		log:     instance.Log(),
		i:       instance,
		ctx:     instance.Context(),
		version: extconfig.FullVersion(),
		jobs:    make(map[string]*activeJob),
		snap:    etcdsnapshot.NewV3(instance.Log().Named("snapshot")),
	}
	r.i.AddEventListener(types.EventTopicAPIMuxSetup, func(ev *roles.Event) {
		svc := ev.Payload.Data["svc"].(*web.Service)
		svc.Post("/api/v1/backup/start", r.APIBackupStart())
		svc.Get("/api/v1/backup/status", r.APIBackupStatus())
		svc.Get("/api/v1/roles/backup", r.APIRoleConfigGet())
		svc.Post("/api/v1/roles/backup", r.APIRoleConfigPut())
	})
	r.i.AddEventListener(EventTopicBackupRun, func(ev *roles.Event) {
		source := SourceEvent
		if s, ok := ev.Payload.Data["source"].(string); ok && s != "" {
			source = s
		}
		r.StartBackup(ev.Context, source, "")
	})
	return r
}

// ensureDirs creates the directory layout used by backups:
//   - tmp/ holds isolated, unpublished staging files,
//   - snapshots/ holds immutable verified snapshot files.
func (r *Role) ensureDirs() error {
	backupDir := extconfig.Get().Dirs().BackupDir
	for _, dir := range []string{backupDir, path.Join(backupDir, dirTemp), path.Join(backupDir, dirSnapshots)} {
		if err := os.MkdirAll(dir, os.ModeSticky|os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

func (r *Role) Start(ctx context.Context, config []byte) error {
	if err := r.ensureDirs(); err != nil {
		return err
	}
	r.cfg = r.decodeRoleConfig(config)
	if r.cfg.Endpoint == "" {
		// No object storage configured: local (API/event triggered)
		// backups remain available, and interrupted local attempts are
		// reconciled just like remote ones.
		r.recoverJobs(ctx)
		return roles.ErrRoleNotConfigured
	}
	endpoint, err := url.Parse(r.cfg.Endpoint)
	if err != nil {
		return err
	}
	opts := &minio.Options{
		Secure:    strings.EqualFold(endpoint.Scheme, "https"),
		Transport: extconfig.Transport(),
	}
	if r.cfg.AccessKey != "" {
		opts.Creds = credentials.NewStaticV4(r.cfg.AccessKey, r.cfg.SecretKey, "")
	} else {
		opts.Creds = credentials.NewChainCredentials(
			[]credentials.Provider{
				&credentials.EnvAWS{},
				&credentials.EnvMinio{},
				&credentials.FileAWSCredentials{},
				&credentials.FileMinioClient{},
				&credentials.IAM{},
			},
		)
	}
	minioClient, err := minio.New(endpoint.Host, opts)
	if err != nil {
		return err
	}
	r.mc = minioClient
	r.ensureBucket()

	// Reconcile interrupted identities before new scheduled triggers can
	// start.
	r.recoverJobs(ctx)

	r.c = cron.New()
	if r.cfg.CronExpr != "" {
		ei, err := r.c.AddFunc(r.cfg.CronExpr, func() {
			ctx, cancel := context.WithTimeout(r.ctx, 60*time.Minute)
			defer cancel()
			r.StartBackup(ctx, SourceCron, "")
		})
		if err != nil {
			return err
		}
		r.log.Info("next backup run", zap.String("next", r.c.Entry(ei).Next.String()))
		r.c.Start()
	}
	return nil
}

func (r *Role) Stop() {
	if r.c != nil {
		<-r.c.Stop().Done()
	}
}

func (r *Role) ensureBucket() {
	exists, err := r.mc.BucketExists(r.ctx, r.cfg.Bucket)
	if err == nil && exists {
		return
	}
	err = r.mc.MakeBucket(r.ctx, r.cfg.Bucket, minio.MakeBucketOptions{})
	if err != nil {
		r.log.Warn("failed to create bucket", zap.Error(err))
	}
}
