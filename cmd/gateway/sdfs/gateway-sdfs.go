/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sdfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
	xnet "github.com/minio/minio/pkg/net"

	"context"
	"time"

	spb "github.com/opendedup/sdfs-client-go/sdfs"
	"google.golang.org/grpc"
)

const (
	sdfsBackend = "sdfs"

	sdfsSeparator = minio.SlashSeparator
)

func init() {
	const sdfsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} SDFS-NAMENODE
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
SDFS-NAMENODE:
  SDFS hostname

EXAMPLES:
  1. Start minio gateway server for SDFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} sdfs://localhost:50051

  2. Start minio gateway server for SDFS with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}} sdfs://localhost:50051
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               sdfsBackend,
		Usage:              "Segmented Dedupe File System (SDFS)",
		Action:             sdfsGatewayMain,
		CustomHelpTemplate: sdfsGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway sdfs' command line.
func sdfsGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, sdfsBackend, 1)
	}

	minio.StartGateway(ctx, &SDFS{args: ctx.Args()})
}

// SDFS implements Gateway.
type SDFS struct {
	args []string
}

type sdfsError struct {
	err       string
	errorCode spb.ErrorCodes
}

func (e *sdfsError) Error() string {
	return fmt.Sprintf("SDFS Error %s %d", e.err, e.errorCode)
}

// Name implements Gateway interface.
func (g *SDFS) Name() string {
	return sdfsBackend
}

// NewGatewayLayer returns hdfs gatewaylayer.
func (g *SDFS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {

	// Contact the server and print out its response.
	// Not addresses found, load it from command line.
	var address string
	var commonPath string
	for _, s := range g.args {
		u, err := xnet.ParseURL(s)
		if err != nil {
			return nil, err
		}
		if u.Scheme != "sdfs" {
			return nil, fmt.Errorf("unsupported scheme %s, only supports sdfs://", u)
		}
		if commonPath != "" && commonPath != u.Path {
			return nil, fmt.Errorf("all namenode paths should be same %s", g.args)
		}
		if commonPath == "" {
			commonPath = u.Path
		}
		address = u.Host
	}

	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("unable to lookup local user: %s", err)
	}
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return nil, fmt.Errorf("unable to initialize sdfsClient")
	}
	vc := spb.NewVolumeServiceClient(conn)
	fc := spb.NewFileIOServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	r, err := fc.MkDirAll(ctx, &spb.MkDirRequest{Path: minio.PathJoin(commonPath, sdfsSeparator, minioMetaTmpBucket)})
	if err != nil {
		return nil, err
	} else if r.GetErrorCode() > 0 {
		return nil, &sdfsError{err: r.GetError(), errorCode: r.GetErrorCode()}
	}
	return &sdfsObjects{clnt: conn, vc: vc, fc: fc, subPath: commonPath, listPool: minio.NewTreeWalkPool(time.Minute * 30)}, nil
}

// Production - hdfs gateway is production ready.
func (g *SDFS) Production() bool {
	return true
}

func (n *sdfsObjects) Shutdown(ctx context.Context) error {
	return n.clnt.Close()
}

func (n *sdfsObjects) StorageInfo(ctx context.Context, _ bool) (si minio.StorageInfo, errs []error) {
	fsInfo, err := n.vc.GetVolumeInfo(ctx, &spb.VolumeInfoRequest{})
	if err != nil {
		return minio.StorageInfo{}, []error{err}
	}
	si.Disks = []madmin.Disk{{UsedSpace: fsInfo.GetDseCompSize()}}
	si.Backend.Type = minio.BackendGateway
	si.Backend.GatewayOnline = !fsInfo.GetOffline()
	return si, nil
}

// hdfsObjects implements gateway for Minio and S3 compatible object storage servers.
type sdfsObjects struct {
	minio.GatewayUnsupported
	clnt     *grpc.ClientConn
	vc       spb.VolumeServiceClient
	fc       spb.FileIOServiceClient
	subPath  string
	listPool *minio.TreeWalkPool
}

func sdfsToObjectErr(ctx context.Context, err sdfsError, params ...string) error {
	if err == nil {
		return nil
	}
	bucket := ""
	object := ""
	uploadID := ""
	switch len(params) {
	case 3:
		uploadID = params[2]
		fallthrough
	case 2:
		object = params[1]
		fallthrough
	case 1:
		bucket = params[0]
	}

	switch err.errorCode {
	case spb.ErrorCodes_ENOENT:
		if uploadID != "" {
			return minio.InvalidUploadID{
				UploadID: uploadID,
			}
		}
		if object != "" {
			return minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.BucketNotFound{Bucket: bucket}
	case spb.ErrorCodes_EEXIST:
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case spb.ErrorCodes_ENOTEMPTY:
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketNotEmpty{Bucket: bucket}
	default:
		logger.LogIf(ctx, err)
		return err
	}
}

func genericToObjectErr(ctx context.Context, err Error, params ...string) error {
	if err == nil {
		return nil
	}

	if _, ok := err.(*sdfsError); ok {
		return sdfsToObjectErr(ctx, err, params)
	}
	logger.LogIf(ctx, err)
	return err

}

// sdfsIsValidBucketName verifies whether a bucket name is valid.
func sdfsIsValidBucketName(bucket string) bool {
	return s3utils.CheckValidBucketNameStrict(bucket) == nil
}

func (n *sdfsObjects) sdfsPathJoin(args ...string) string {
	return minio.PathJoin(append([]string{n.subPath, sdfsSeparator}, args...)...)
}

func (n *sdfsObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if !sdfsIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	rc, err := n.fc.RmDir(ctx, &spb.RmDirRequest{Path: n.sdfsPathJoin(bucket)})

	if err != null {
		logger.LogIf(ctx, err)
		return sdfsToObjectErr(ctx, err, bucket)
	} else if rc.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx, &sdfsError{err: rc.GetError(), errorCode: rc.GetErrorCode()}, bucket)
	} else {
		return nil
	}
}

func (n *sdfsObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}

	rc, err := n.fc.MkDir(ctx, &spb.MkDirRequest{Path: n.sdfsPathJoin(bucket)})

	if err != null {
		logger.LogIf(ctx, err)
		return sdfsToObjectErr(ctx, err, bucket)
	} else if rc.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx, sdfsError{err: rc.GetError(), errorCode: rc.GetErrorCode()}, bucket)
	} else {
		return nil
	}
}

func (n *sdfsObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	fi, err := n.fc.Stat(ctx, &spb.FileInfoRequest{FileName: n.sdfsPathJoin(bucket)})
	if err != null {
		logger.LogIf(ctx, err)
		return sdfsToObjectErr(ctx, err, bucket)
	} else if fi.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx, sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
	}
	return minio.BucketInfo{Name: bucket, Created: time.Unix(0, fi.GetResponse()[0].GetCtime()*int64(1000000))}, nil
}

func (n *sdfsObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	fi, err := n.fc.GetFileInfo(ctx, &spb.FileInfoRequest{FileName: sdfsSeparator})

	if err != nil {
		logger.LogIf(ctx, err)
		return nil, sdfsToObjectErr(ctx, err)
	} else if fi.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx, sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
	}
	entries := fi.GetResponse()
	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry.Name(), false) {
			continue
		}
		buckets = append(buckets, minio.BucketInfo{
			Name:    entry.GetFileName(),
			Created: time.Unix(0, entry.GetMtime()*int64(1000000)),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(buckets))
	return buckets, nil
}

func (n *sdfsObjects) listDirFactory() minio.ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string) {
		fi, err := n.fc.GetFileInfo(ctx, &spb.FileInfoRequest{FileName: n.sdfsPathJoin(bucket, prefixDir), NumberOfFiles: 1000000, Compact: true})
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, sdfsToObjectErr(ctx, err)
		} else if fi.GetErrorCode() > 0 {
			return sdfsToObjectErr(ctx, sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
		}
		if len(fi.GetResponse()) == 0 {
			return true, nil
		}
		fis := fi.GetResponse()
		for _, fl := range fis {
			if fl.GetType() == spb.FileInfoResponse_DIR {
				entries = append(entries, fl.GetFileName()+sdfsSeparator)
			} else {
				entries = append(entries, fl.GetFileName())
			}
		}
		return false, minio.FilterMatchingPrefix(entries, prefixEntry)
	}

	// Return list factory instance.
	return listDir
}

// ListObjects lists all blobs in SDFS bucket filtered by prefix.
func (n *sdfsObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	fi, err := n.fc.FileExists(ctx, &spb.FileExistsRequest{Path: n.sdfsPathJoin(bucket)})
	if err != nil {
		return loi, sdfsToObjectErr(ctx, err, bucket)
	} else if fi.GetErrorCode() > 0 {
		return lio, sdfsToObjectErr(ctx, sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
	}

	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		fl, err := n.fc.Stat(ctx, &spb.FileInfoRequest{FileName: entry})

		if err != nil {
			return minio.ObjectInfo{}, sdfsObjectErr(ctx, err, bucket, entry)
		} else if fl.GetErrorCode() > 0 {
			return minio.ObjectInfo{}, sdfsToObjectErr(ctx, sdfsError{err: fl.GetError(), errorCode: fl.GetErrorCode()}, bucket)
		}
		var dir bool = false
		if fl.GetResponse()[0].GetType() == spb.FileInfoResponse_DIR {
			dir = true
		}

		return minio.ObjectInfo{
			Bucket:  bucket,
			Name:    entry,
			Size:    fl.GetResponse()[0].GetSize(),
			IsDir:   dir,
			ModTime: time.Unix(0, fl.GetResponse()[0].GetMtime()*int64(1000000)),
			AccTime: time.Unix(0, fl.GetResponse()[0].GetAtime()*int64(1000000)),
		}, nil
	}

	return minio.ListObjects(ctx, n, bucket, prefix, marker, delimiter, maxKeys, n.listPool, n.listDirFactory(), getObjectInfo, getObjectInfo)
}

// deleteObject deletes a file path if its empty. If it's successfully deleted,
// it will recursively move up the tree, deleting empty parent directories
// until it finds one with files in it. Returns nil for a non-empty directory.
func (n *sdfsObjects) deleteObject(ctx context.Context, basePath, deletePath string) error {
	if basePath == deletePath {
		return nil
	}
	// Attempt to remove path.
	fi, err := n.fc.Stat(ctx, &spb.FileInfoRequest{FileName: deletePath})
	if err != nil {
		logger.LogIf(ctx, err)
		return nil
	} else if fi.GetErrorCode() > 0 {
		err = sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}
		logger.LogIf(ctx, err)
		return nil
	}
	if fi.GetResponse()[0].GetType == spb.FileInfoResponse_DIR {
		fd, err := n.fc.RmDir(ctx, &spb.RmDirRequest{Path: deletePath})
		if err != nil {
			return err
		} else if fd.GetErrorCode() > 0 {
			if fd.GetErrorCode() == spb.ErrorCodes_ENOTEMPTY {
				return nil
			}
			err = sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}
			logger.LogIf(ctx, err)
			return err
		}
	} else {
		fd, err := n.fc.Unlink(ctx, &spb.UnlinkRequest{Path: deletePath})
		if err != nil {
			return err
		} else if fd.GetErrorCode() > 0 {

			err = sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}
			logger.LogIf(ctx, err)
			return err
		}
	}

	// Trailing slash is removed when found to ensure
	// slashpath.Dir() to work as intended.
	deletePath = strings.TrimSuffix(deletePath, sdfsSeparator)
	deletePath = path.Dir(deletePath)

	// Delete parent directory. Errors for parent directories shouldn't trickle down.
	n.deleteObject(ctx, basePath, deletePath)
	return nil
}

// ListObjectsV2 lists all blobs in SDFS bucket filtered by prefix
func (n *sdfsObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := n.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}
	return minio.ListObjectsV2Info{
		Objects:               resultV1.Objects,
		Prefixes:              resultV1.Prefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: resultV1.NextMarker,
		IsTruncated:           resultV1.IsTruncated,
	}, nil
}

func (n *sdfsObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	err := sdfsToObjectErr(ctx, n.deleteObject(ctx, n.sdfsPathJoin(bucket), n.sdfsPathJoin(bucket, object)), bucket, object)
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, err

}

func (n *sdfsObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {

	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = n.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return dobjects, errs
}

func (n *sdfsObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	objInfo, err := n.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}
	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		fr, err := n.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)

}

func (n *sdfsObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	cpSrcDstSame := minio.IsStringEqual(n.sdfsPathJoin(srcBucket, srcObject), n.sdfsPathJoin(dstBucket, dstObject))
	if cpSrcDstSame {
		return n.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}

	fcc, err := n.fc.CreateCopy(ctx, &spb.FileSnapshotRequest{
		Src:  n.sdfsPathJoin(srcBucket, srcObject),
		Dest: n.sdfsPathJoin(dstBucket, dstObject),
	})
	if err != nil {
		return objInfo, sdfsToObjectErr(ctx, err, bucket)
	} else if fcc.GetErrorCode() > 0 {
		return objInfo, sdfsToObjectErr(ctx, sdfsError{err: fcc.GetError(), errorCode: fcc.GetErrorCode()}, bucket)
	} else {
		return n.GetObjectInfo(ctx, dstBucket, dstObject, minio.ObjectOptions{})
	}

}

func (n *sdfsObjects) GetObject(ctx context.Context, bucket, key string, startOffset int64, length int32, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return sdfsToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return dfsToObjectErr(ctx, sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}
	rd, err := n.fc.Open(ctx, &spb.FileOpenRequest{Path: n.sdfsPathJoin(bucket, key)})
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket, key)
	} else if rd.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx, sdfsError{err: rd.GetError(), errorCode: rd.GetErrorCode()}, bucket)
	}
	defer n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: rd.GetFileHandle()})
	rdr, err := n.fc.Read(ctx, &spb.DataReadRequest{FileHandle: rd.GetFileHandle(), Len: length, Start: startOffset})
	_, err = writer.Write(rdr.GetData())
	return sdfsToObjectErr(ctx, err, bucket, key)
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (n *sdfsObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return objInfo, sdfsToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return objInfo, sdfsToObjectErr(ctx, sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}
	if strings.HasSuffix(object, sdfsSeparator) && !fi.GetResponse()[0].GetType() == spb.FileInfoResponse_DIR {
		return objInfo, sdfsToObjectErr(ctx, sdfsError{
			err:       "object does not exist",
			errorCode: spb.ErrorCodes_ENOENT,
		}, bucket)
	}

	fi, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.sdfsPathJoin(bucket, object)})
	if err != nil {
		return objInfo, sdfsToObjectErr(ctx, err, bucket, object)
	} else if fi.GetErrorCode() > 0 {
		return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: time.Unix(0, fi.GetResponse()[0].GetMtime()*int64(1000000)),
		Size:    fi.Size(),
		IsDir:   fi.GetResponse()[0].GetType() == spb.FileInfoResponse_DIR,
		AccTime: time.Unix(0, fi.GetResponse()[0].GetAtime()*int64(1000000)),
	}, nil
}

func (n *sdfsObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return objInfo, sdfsToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}

	name := n.sdfsPathJoin(bucket, object)

	// If its a directory create a prefix {
	if strings.HasSuffix(object, sdfsSeparator) && r.Size() == 0 {
		mkd, err := n.fc.MkDirAll(ctx, &spb.MkDirRequest{Path: name})
		if err != nil {
			return objInfo, sdfsToObjectErr(ctx, err, bucket)
		} else if fb.GetErrorCode() > 0 {
			return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
		}
	} else {
		tmpname := n.sdfsPathJoin(minioMetaTmpBucket, minio.MustGetUUID())
		var fh int64
		mkf, err := n.fc.Mknod(ctx, &spb.MkNodRequest{Path: tmpname})
		if err != nil {
			return objInfo, sdfsToObjectErr(ctx, err, bucket)
		} else if mkf.GetErrorCode() > 0 {
			return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: mkf.GetError(), errorCode: mkf.GetErrorCode()}, bucket)
		}
		fhr, err := n.fc.Open(ctx, &spb.FileOpenRequest{Path: tmpname})
		if err != nil {
			return objInfo, sdfsToObjectErr(ctx, err, bucket)
		} else if fhr.GetErrorCode() > 0 {
			return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: fhr.GetError(), errorCode: fhr.GetErrorCode()}, bucket)
		}
		defer n.fc.Unlink(ctx, &spb.UnlinkRequest{Path: tmpname})
		fh = fhr.GetFileHandle()
		b1 := make([]byte, 128*1024)
		var offset int64 = 0
		var n1 int = 0
		n1, err = r.Read(b1)
		fwr, err := n.fc.Write(ctx, &spb.DataWriteRequest{FileHandle: fh, Data: b1, Start: offset, Len: int32(n1)})
		offset += n1
		if err != nil {
			n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
			return objInfo, sdfsToObjectErr(ctx, err, bucket)
		} else if fwr.GetErrorCode() > 0 {
			n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
			return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: fwr.GetError(), errorCode: fwr.GetErrorCode()}, bucket)
		}
		for n1 == len(b1) {
			n1, err = r.Read(b1)
			fwr, err = n.fc.Write(ctx, &spb.DataWriteRequest{FileHandle: fh, Data: b1, Start: offset, Len: int32(n1)})
			offset += n1
			if err != nil {
				n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
				return objInfo, sdfsToObjectErr(ctx, err, bucket)
			} else if fwr.GetErrorCode() > 0 {
				n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
				return objInfo, sdfsToObjectErr(ctx, sdfsError{err: fwr.GetError(), errorCode: fwr.GetErrorCode()}, bucket)
			}
		}
		n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
		dir := path.Dir(name)
		if dir != "" {
			mkd, err = n.fc.MkDirAll(ctx, &spb.MkDirRequest{Path: dir})
			if err != nil {
				return objInfo, sdfsToObjectErr(ctx, err, bucket)
			} else if mkd.GetErrorCode() > 0 {
				return objInfo, sdfsToObjectErr(ctx, sdfsError{err: mkd.GetError(), errorCode: mkd.GetErrorCode()}, bucket)
			}
		}

		_, err = n.fc.Rename(ctx, &spb.FileRenameRequest{Src: tmpname, Dest: name})

	}

	fi, err := n.fc.Stat(ctx, &spb.FileInfoRequest{FileName: name})
	if err != nil {
		return objInfo, sdfsToObjectErr(ctx, err, bucket, object)
	} else if fi.GetErrorCode() > 0 {
		return objInfo, sdfsToObjectErr(ctx, sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: time.Unix(0, fi.GetResponse()[0].GetMtime()*int64(1000000)),
		Size:    fi.Size(),
		IsDir:   fi.GetResponse()[0].GetType() == spb.FileInfoResponse_DIR,
		AccTime: time.Unix(0, fi.GetResponse()[0].GetAtime()*int64(1000000)),
	}, nil
}

func (n *sdfsObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return uploadID, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return uploadID, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}

	uploadID = minio.MustGetUUID()
	fcr, err := n.fc.Mknod(ctx, &spb.MkNodRequest{n.sdfsPathJoin(minioMetaTmpBucket, uploadID)})
	if err != nil {
		return uploadID, genericToObjectErr(ctx, err, bucket)
	} else if fcr.GetErrorCode() > 0 {
		return uploadID, genericToObjectErr(ctx, &sdfsError{err: fcr.GetError(), errorCode: fcr.GetErrorCode()}, bucket)
	}
	return uploadID, nil
}

func (n *sdfsObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return lmi, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return lmi, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}

	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return lmi, nil
}

func (n *sdfsObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	fb, err := n.fc.Stat(n.sdfsPathJoin(minioMetaTmpBucket, uploadID))
	if err != nil {
		return sdfsToObjectErr(ctx, err, bucket, object, uploadID)
	} else if fb.GetErrorCode() > 0 {
		return genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}
	return nil
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (n *sdfsObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return result, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return result, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return result, nil
}

func (n *sdfsObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return result, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return result, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	// It's decided not to support List parts, hence returning empty result.
	return result, nil
}

func (n *sdfsObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return nil, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return nil, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *sdfsObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return info, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return result, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}
	fi, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.sdfsPathJoin(minioMetaTmpBucket, uploadID)})
	if err != nil {
		return info, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return result, genericToObjectErr(ctx, &sdfsError{err: fi.GetError(), errorCode: fi.GetErrorCode()}, bucket)
	}
	fhr, err := n.fc.Open(ctx, &spb.FileOpenRequest{Path: n.sdfsPathJoin(minioMetaTmpBucket, uploadID)})
	offset := fi.GetResponse()[0].GetSize()
	fh = fhr.GetFileHandle()
	b1 := make([]byte, 128*1024)
	var offset int64 = 0
	var n1 int = 0
	n1, err = r.Read(b1)
	fwr, err := n.fc.Write(ctx, &spb.DataWriteRequest{FileHandle: fh, Data: b1, Start: offset, Len: int32(n1)})
	offset += n1
	if err != nil {
		n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
		return objInfo, sdfsToObjectErr(ctx, err, bucket)
	} else if fwr.GetErrorCode() > 0 {
		n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
		return objInfo, sdfsToObjectErr(ctx, &sdfsError{err: fwr.GetError(), errorCode: fwr.GetErrorCode()}, bucket)
	}
	for n1 == len(b1) {
		n1, err = r.Read(b1)
		fwr, err = n.fc.Write(ctx, &spb.DataWriteRequest{FileHandle: fh, Data: b1, Start: offset, Len: int32(n1)})
		offset += n1
		if err != nil {
			n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
			return objInfo, sdfsToObjectErr(ctx, err, bucket)
		} else if fwr.GetErrorCode() > 0 {
			n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
			return objInfo, sdfsToObjectErr(ctx, sdfsError{err: fwr.GetError(), errorCode: fwr.GetErrorCode()}, bucket)
		}
	}
	n.fc.Release(ctx, &spb.FileCloseRequest{FileHandle: fh})
	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

	return info, nil
}

func (n *sdfsObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fb, err := n.fc.Stat(ctx, &spb.FileInfoRequest{Path: n.PathJoin(bucket)})
	if err != nil {
		return info, genericToObjectErr(ctx, err, bucket)
	} else if fb.GetErrorCode() > 0 {
		return result, genericToObjectErr(ctx, &sdfsError{err: fb.GetError(), errorCode: fb.GetErrorCode()}, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	name := n.sdfsPathJoin(bucket, object)
	dir := path.Dir(name)
	if dir != "" {
		fd, err := n.fc.MkDirAll(ctx,&spb.MkDirRequest{Path:dir})
		if err = {
			return objInfo, genericToObjectErr(ctx, err, bucket, object)
		}
		if fd.GetErrorCode() > 0 {
			return objInfo, genericToObjectErr(ctx, &sdfsError{err: fd.GetError(), errorCode: fd.GetErrorCode()}, bucket)
		}
	}
	fr, err := n.fc.Rename(ctx,&spb.FileRenameRequest{Src:n.sdfsPathJoin(minioMetaTmpBucket, uploadID),Dest:name})
	if err != nil {
		return objInfo, sdfsToObjectErr(ctx, err, bucket, object)
	}
	if fr.GetErrorCode() == spb.ErrorCodes_EEXIST {
		frr,err := n.fc.RmDir(name)
		if err != nil {
			if dir != "" {
				n.deleteObject(ctx,n.sdfsPathJoin(bucket), dir)
			}
			return objInfo, sdfsToObjectErr(ctx, err, bucket, object)
		}
		
		frn,err := n.fc.Rename(ctx,&spb.FileRenameRequest{Src: n.sdfsPathJoin(minioMetaTmpBucket, uploadID),Dest: name})
		if err != nil {
			return objInfo, sdfsToObjectErr(ctx, err, bucket, object)
		}

		if err = n.clnt.Rename(n.hdfsPathJoin(minioMetaTmpBucket, uploadID), name); err != nil {
			if dir != "" {
				n.deleteObject(n.hdfsPathJoin(bucket), dir)
			}
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	}
	fi, err := n.clnt.Stat(name)
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := minio.ComputeCompleteMultipartMD5(parts)

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    s3MD5,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}, nil
}


func (n *hdfsObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket)
	}
	return hdfsToObjectErr(ctx, n.clnt.Remove(n.hdfsPathJoin(minioMetaTmpBucket, uploadID)), bucket, object, uploadID)
}
