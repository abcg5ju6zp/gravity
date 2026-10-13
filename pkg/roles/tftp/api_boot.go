package tftp

import (
	"context"

	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
)

// APIBootBundleOutput wraps a single bundle descriptor.
type APIBootBundleOutput struct {
	Bundle BootBundle `json:"bundle" required:"true"`
}

// APIBootBundlesListOutput lists all boot bundles.
type APIBootBundlesListOutput struct {
	Bundles []BootBundle `json:"bundles" required:"true"`
}

// APIBootBundleCreateInput creates (or idempotently returns) a named bundle.
type APIBootBundleCreateInput struct {
	Name string `json:"name" required:"true"`
}

func (r *Role) APIBootBundleCreate() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootBundleCreateInput, output *APIBootBundleOutput) error {
		b, err := r.boot.createBundle(ctx, input.Name)
		if err != nil {
			return err
		}
		output.Bundle = *b
		return nil
	})
	u.SetName("tftp.create_boot_bundle")
	u.SetTitle("Create boot bundle")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.Internal)
	return u
}

func (r *Role) APIBootBundlesList() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *APIBootBundlesListOutput) error {
		bundles, err := r.boot.listBundles(ctx)
		if err != nil {
			return err
		}
		output.Bundles = bundles
		return nil
	})
	u.SetName("tftp.list_boot_bundles")
	u.SetTitle("List boot bundles")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.Internal)
	return u
}

// APIBootVersionOutput is the queryable state of one version.
type APIBootVersionOutput struct {
	Version BootVersion `json:"version" required:"true"`
	// Missing lists required manifest paths not yet received.
	Missing []string `json:"missing" required:"true"`
}

// APIBootVersionCreateInput opens a candidate version with a client-supplied
// stable identity and an immutable file manifest.
type APIBootVersionCreateInput struct {
	Bundle  string         `json:"bundle" required:"true"`
	Version string         `json:"version" required:"true"`
	Files   []BootFileSpec `json:"files" required:"true"`
}

// APIBootVersionsListInput lists all versions of one bundle.
type APIBootVersionsListInput struct {
	Bundle string `query:"bundle" required:"true"`
}

// APIBootVersionLookupInput addresses one version of a bundle.
type APIBootVersionLookupInput struct {
	Bundle  string `query:"bundle" required:"true"`
	Version string `query:"version" required:"true"`
}

// APIBootVersionsListOutput lists versions with their derived completeness.
type APIBootVersionsListOutput struct {
	Versions []APIBootVersionOutput `json:"versions" required:"true"`
}

// APIBootVersionCancelInput cancels an open candidate.
type APIBootVersionCancelInput struct {
	Bundle  string `json:"bundle" required:"true"`
	Version string `json:"version" required:"true"`
}

// APIBootVersionFilePutInput uploads one file into a candidate version.
type APIBootVersionFilePutInput struct {
	Bundle  string `json:"bundle" required:"true"`
	Version string `json:"version" required:"true"`
	Path    string `json:"path" required:"true"`
	Data    []byte `json:"data" required:"true"`
}

// APIBootVersionFileDownloadInput fetches one received file of any version.
type APIBootVersionFileDownloadInput struct {
	Bundle  string `query:"bundle" required:"true"`
	Version string `query:"version" required:"true"`
	Path    string `query:"path" required:"true"`
}

// APIBootVersionFileDownloadOutput carries the raw file bytes.
type APIBootVersionFileDownloadOutput struct {
	Data []byte `json:"data" required:"true"`
}

func bootVersionOutput(v *BootVersion) APIBootVersionOutput {
	return APIBootVersionOutput{
		Version: *v,
		Missing: v.Missing(),
	}
}

func (r *Role) APIBootVersionCreate() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionCreateInput, output *APIBootVersionOutput) error {
		v, err := r.boot.createVersion(ctx, input.Bundle, input.Version, input.Files)
		if err != nil {
			return err
		}
		*output = bootVersionOutput(v)
		return nil
	})
	u.SetName("tftp.create_boot_version")
	u.SetTitle("Create boot bundle candidate version")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.AlreadyExists, status.Internal)
	return u
}

func (r *Role) APIBootVersionsList() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionsListInput, output *APIBootVersionsListOutput) error {
		versions, err := r.boot.listVersions(ctx, input.Bundle)
		if err != nil {
			return err
		}
		output.Versions = make([]APIBootVersionOutput, len(versions))
		for i := range versions {
			output.Versions[i] = bootVersionOutput(&versions[i])
		}
		return nil
	})
	u.SetName("tftp.list_boot_versions")
	u.SetTitle("List boot bundle versions")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.Internal)
	return u
}

func (r *Role) APIBootVersionStatus() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionLookupInput, output *APIBootVersionOutput) error {
		v, _, err := r.boot.loadVersion(ctx, input.Bundle, input.Version)
		if err != nil {
			return err
		}
		*output = bootVersionOutput(v)
		return nil
	})
	u.SetName("tftp.get_boot_version")
	u.SetTitle("Get boot bundle version status")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.Internal)
	return u
}

func (r *Role) APIBootVersionFilePut() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionFilePutInput, output *struct{}) error {
		return r.boot.putVersionFile(ctx, input.Bundle, input.Version, input.Path, input.Data)
	})
	u.SetName("tftp.put_boot_version_file")
	u.SetTitle("Upload a boot bundle candidate file")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.AlreadyExists, status.FailedPrecondition, status.Internal)
	return u
}

func (r *Role) APIBootVersionFileDownload() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionFileDownloadInput, output *APIBootVersionFileDownloadOutput) error {
		data, err := r.boot.downloadVersionFile(ctx, input.Bundle, input.Version, input.Path)
		if err != nil {
			return err
		}
		output.Data = data
		return nil
	})
	u.SetName("tftp.download_boot_version_file")
	u.SetTitle("Download a boot bundle version file")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.Internal)
	return u
}

// APIBootVersionPublishInput atomically publishes a complete candidate.
// IfCurrentVersion optionally fences the publish: it only succeeds while the
// named version is the current one.
type APIBootVersionPublishInput struct {
	Bundle           string `json:"bundle" required:"true"`
	Version          string `json:"version" required:"true"`
	IfCurrentVersion string `json:"ifCurrentVersion,omitempty"`
}

// APIBootPublishOutput is the reconciled publish outcome. State is
// "published" on success; a retried identity overtaken by another publish
// returns "superseded" with the actual current pointer and its history seq.
type APIBootPublishOutput struct {
	Bundle           string `json:"bundle" required:"true"`
	VersionID        string `json:"versionId" required:"true"`
	State            string `json:"state" required:"true"`
	CurrentVersionID string `json:"currentVersionId" required:"true"`
	PublishSeq       int64  `json:"publishSeq" required:"true"`
}

func (r *Role) APIBootVersionPublish() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionPublishInput, output *APIBootPublishOutput) error {
		res, err := r.boot.publishVersion(ctx, input.Bundle, input.Version, input.IfCurrentVersion)
		if err != nil {
			return err
		}
		*output = *res
		return nil
	})
	u.SetName("tftp.publish_boot_version")
	u.SetTitle("Publish a boot bundle candidate version")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.FailedPrecondition, status.Aborted, status.Internal)
	return u
}

// APIBootGCOutput lists versions removed by one garbage collection run.
type APIBootGCOutput struct {
	Deleted []APIBootDeletedVersion `json:"deleted" required:"true"`
}

func (r *Role) APIBootGC() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *APIBootGCOutput) error {
		deleted, err := r.boot.garbageCollect(ctx)
		if err != nil {
			return err
		}
		output.Deleted = deleted
		return nil
	})
	u.SetName("tftp.gc_boot_versions")
	u.SetTitle("Garbage collect unreferenced boot bundle versions")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.Internal)
	return u
}

func (r *Role) APIBootVersionCancel() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIBootVersionCancelInput, output *APIBootVersionOutput) error {
		v, err := r.boot.cancelVersion(ctx, input.Bundle, input.Version)
		if err != nil {
			return err
		}
		*output = bootVersionOutput(v)
		return nil
	})
	u.SetName("tftp.cancel_boot_version")
	u.SetTitle("Cancel a boot bundle candidate version")
	u.SetTags("roles/tftp")
	u.SetExpectedErrors(status.NotFound, status.FailedPrecondition, status.Internal)
	return u
}
