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
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"syscall"
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
	r, err := fc.MkDirAll(ctx, &spb.MkDirRequest{
		Path: minio.PathJoin(commonPath, sdfsSeparator, minioMetaTmpBucket)
	})
	if err != nil {
		return nil, err
	} else if r.GetErrorCode() > 0 {
		return nil, &sdfsError{err: r.GetError(),errorCode: r.GetErrorCode()}
	}
	return &sdfsObjects{clnt: conn, vc:vc, fc:fc, subPath: commonPath, listPool: minio.NewTreeWalkPool(time.Minute * 30)}, nil
}

// Production - hdfs gateway is production ready.
func (g *SDFS) Production() bool {
	return true
}

func (n *sdfsObjects) Shutdown(ctx context.Context) error {
	return n.clnt.Close()
}

func (n *sdfsObjects) StorageInfo(ctx context.Context, _ bool) (si minio.StorageInfo, errs []error) {
	fsInfo, err := n.vc.GetVolumeInfo(ctx,&spb.VolumeInfoRequest{})
	if err != nil {
		return minio.StorageInfo{}, []error{err}
	}
	si.Disks = []madmin.Disk{{
		UsedSpace: fsInfo.GetDseCompSize()
	}}
	si.Backend.Type = minio.BackendGateway
	si.Backend.GatewayOnline = !fsInfo.GetOffline();
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
	rc,err := n.fc.RmDir(ctx,&spb.RmDirRequest{Path: n.sdfsPathJoin(bucket)})
	
	if err != null {
		logger.LogIf(ctx, err)
		return sdfsToObjectErr(ctx, err, bucket)
	} else if rc.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx,sdfsError{err: rc.GetError(),errorCode: rc.GetErrorCode()}, bucket)
	} else {
		return nil
	}
}

func (n *sdfsObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}

	rc,err := n.fc.MkDir(ctx,&spb.MkDirRequest{Path: n.sdfsPathJoin(bucket)})
	
	if err != null {
		logger.LogIf(ctx, err)
		return sdfsToObjectErr(ctx, err, bucket)
	} else if rc.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx,sdfsError{err: rc.GetError(),errorCode: rc.GetErrorCode()}, bucket)
	} else {
		return nil
	}
}

func (n *sdfsObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	fi, err := n.fc.GetFileInfo(ctx,&spb.FileInfoRequest{FileName:n.sdfsPathJoin(bucket)})
	if err != null {
		logger.LogIf(ctx, err)
		return sdfsToObjectErr(ctx, err, bucket)
	} else if fi.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx,sdfsError{err: fi.GetError(),errorCode: fi.GetErrorCode()}, bucket)
	}
	// As hdfs.Stat() doesn't carry anything other than ModTime(), use ModTime() as CreatedTime.
	return minio.BucketInfo{
		Name:    bucket,
		Created: time.Unix(0, fi.GetResponse()[0].GetMtime() * int64(1000000)) 
	}, nil
}

func (n *sdfsObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	fi, err := n.fc.GetFileInfo(ctx,&spb.FileInfoRequest{FileName:sdfsSeparator})
	
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, sdfsToObjectErr(ctx, err)
	} else if fi.GetErrorCode() > 0 {
		return sdfsToObjectErr(ctx,sdfsError{err: fi.GetError(),errorCode: fi.GetErrorCode()}, bucket)
	}
	entries := fi.GetResponse() 
	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry.Name(), false) {
			continue
		}
		buckets = append(buckets, minio.BucketInfo{
			Name: entry.GetFileName(),
			// As hdfs.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: time.Unix(0, entry.GetMtime() * int64(1000000)),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(buckets))
	return buckets, nil
}

func (n *sdfsObjects) listDirFactory() minio.ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string) {
		fi, err := n.fc.GetFileInfo(ctx,&spb.FileInfoRequest{FileName:n.sdfsPathJoin(bucket, prefixDir),NumberOfFiles:1000000,Compact:true})
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, sdfsToObjectErr(ctx, err)
		} else if fi.GetErrorCode() > 0 {
			return sdfsToObjectErr(ctx,sdfsError{err: fi.GetError(),errorCode: fi.GetErrorCode()}, bucket)
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
	fi, err := n.fc.FileExists(ctx,&spb.FileExistsRequest{Path:n.sdfsPathJoin(bucket)})
	if err != nil {
		return loi, hdfsToObjectErr(ctx, err, bucket)
	} else if fi.GetErrorCode() > 0 {
		return lio, sdfsToObjectErr(ctx,sdfsError{err: fi.GetError(),errorCode: fi.GetErrorCode()}, bucket)
	} else if !fi.GetExists() {
		return lio, sdfsToObjectErr(ctx,sdfsError{err: "bucket does not exists ["+bucket+"]", errorCode: spb.ErrorCodes_ENOENT}, bucket)
	}
	


	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		fl, err := n.fc.GetFileInfo(ctx,&spb.FileInfoRequest{FileName:entry})
		
		if err != nil {
			return minio.ObjectInfo{}, sdfsObjectErr(ctx, err, bucket, entry)
		}else if fl.GetErrorCode() > 0 {
			return minio.ObjectInfo{}, sdfsToObjectErr(ctx,sdfsError{err: fl.GetError(),errorCode: fl.GetErrorCode()}, bucket)
		}
		var dir bool = false;
		if fl.GetResponse()[0].GetType() == spb.FileInfoResponse_DIR {
			dir = true
		}

		return minio.ObjectInfo{
			Bucket:  bucket,
			Name:    entry,
			Size:    fl.GetResponse()[0].GetSize(),
			IsDir:   dir,
			ModTime: time.Unix(0, fl.GetResponse()[0].GetMtime() * int64(1000000)),
			AccTime: time.Unix(0, fl.GetResponse()[0].GetAtime() * int64(1000000)),
		}, nil
	}

	return minio.ListObjects(ctx, n, bucket, prefix, marker, delimiter, maxKeys, n.listPool, n.listDirFactory(), getObjectInfo, getObjectInfo)
}

// deleteObject deletes a file path if its empty. If it's successfully deleted,
// it will recursively move up the tree, deleting empty parent directories
// until it finds one with files in it. Returns nil for a non-empty directory.
func (n *hdfsObjects) deleteObject(basePath, deletePath string) error {
	if basePath == deletePath {
		return nil
	}

	// Attempt to remove path.
	if err := n.clnt.Remove(deletePath); err != nil {
		if errors.Is(err, syscall.ENOTEMPTY) {
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		}
		return err
	}

	// Trailing slash is removed when found to ensure
	// slashpath.Dir() to work as intended.
	deletePath = strings.TrimSuffix(deletePath, hdfsSeparator)
	deletePath = path.Dir(deletePath)

	// Delete parent directory. Errors for parent directories shouldn't trickle down.
	n.deleteObject(basePath, deletePath)

	return nil
}

// ListObjectsV2 lists all blobs in HDFS bucket filtered by prefix
func (n *hdfsObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
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

func (n *hdfsObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	err := hdfsToObjectErr(ctx, n.deleteObject(n.hdfsPathJoin(bucket), n.hdfsPathJoin(bucket, object)), bucket, object)
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, err
}

func (n *hdfsObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
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

func (n *hdfsObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
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
		nerr := n.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)

}

func (n *hdfsObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	cpSrcDstSame := minio.IsStringEqual(n.hdfsPathJoin(srcBucket, srcObject), n.hdfsPathJoin(dstBucket, dstObject))
	if cpSrcDstSame {
		return n.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}

	return n.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, minio.ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
	})
}

func (n *hdfsObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	if _, err := n.clnt.Stat(n.hdfsPathJoin(bucket)); err != nil {
		return hdfsToObjectErr(ctx, err, bucket)
	}
	rd, err := n.clnt.Open(n.hdfsPathJoin(bucket, key))
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket, key)
	}
	defer rd.Close()
	_, err = io.Copy(writer, io.NewSectionReader(rd, startOffset, length))
	if err == io.ErrClosedPipe {
		// hdfs library doesn't send EOF correctly, so io.Copy attempts
		// to write which returns io.ErrClosedPipe - just ignore
		// this for now.
		err = nil
	}
	return hdfsToObjectErr(ctx, err, bucket, key)
}

func (n *hdfsObjects) isObjectDir(ctx context.Context, bucket, object string) bool {
	f, err := n.clnt.Open(n.hdfsPathJoin(bucket, object))
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		logger.LogIf(ctx, err)
		return false
	}
	defer f.Close()
	fis, err := f.Readdir(1)
	if err != nil && err != io.EOF {
		logger.LogIf(ctx, err)
		return false
	}
	// Readdir returns an io.EOF when len(fis) == 0.
	return len(fis) == 0
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (n *hdfsObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket)
	}
	if strings.HasSuffix(object, hdfsSeparator) && !n.isObjectDir(ctx, bucket, object) {
		return objInfo, hdfsToObjectErr(ctx, os.ErrNotExist, bucket, object)
	}

	fi, err := n.clnt.Stat(n.hdfsPathJoin(bucket, object))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}, nil
}

func (n *hdfsObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket)
	}

	name := n.hdfsPathJoin(bucket, object)

	// If its a directory create a prefix {
	if strings.HasSuffix(object, hdfsSeparator) && r.Size() == 0 {
		if err = n.clnt.MkdirAll(name, os.FileMode(0755)); err != nil {
			n.deleteObject(n.hdfsPathJoin(bucket), name)
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	} else {
		tmpname := n.hdfsPathJoin(minioMetaTmpBucket, minio.MustGetUUID())
		var w *hdfs.FileWriter
		w, err = n.clnt.Create(tmpname)
		if err != nil {
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
		defer n.deleteObject(n.hdfsPathJoin(minioMetaTmpBucket), tmpname)
		if _, err = io.Copy(w, r); err != nil {
			w.Close()
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
		dir := path.Dir(name)
		if dir != "" {
			if err = n.clnt.MkdirAll(dir, os.FileMode(0755)); err != nil {
				w.Close()
				n.deleteObject(n.hdfsPathJoin(bucket), dir)
				return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
			}
		}
		w.Close()
		if err = n.clnt.Rename(tmpname, name); err != nil {
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	}
	fi, err := n.clnt.Stat(name)
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}, nil
}

func (n *hdfsObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return uploadID, hdfsToObjectErr(ctx, err, bucket)
	}

	uploadID = minio.MustGetUUID()
	if err = n.clnt.CreateEmptyFile(n.hdfsPathJoin(minioMetaTmpBucket, uploadID)); err != nil {
		return uploadID, hdfsToObjectErr(ctx, err, bucket)
	}

	return uploadID, nil
}

func (n *hdfsObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return lmi, hdfsToObjectErr(ctx, err, bucket)
	}

	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return lmi, nil
}

func (n *hdfsObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(minioMetaTmpBucket, uploadID))
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket, object, uploadID)
	}
	return nil
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (n *hdfsObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return result, hdfsToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return result, nil
}

func (n *hdfsObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return result, hdfsToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	// It's decided not to support List parts, hence returning empty result.
	return result, nil
}

func (n *hdfsObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *hdfsObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return info, hdfsToObjectErr(ctx, err, bucket)
	}

	var w *hdfs.FileWriter
	w, err = n.clnt.Append(n.hdfsPathJoin(minioMetaTmpBucket, uploadID))
	if err != nil {
		return info, hdfsToObjectErr(ctx, err, bucket, object, uploadID)
	}
	defer w.Close()
	_, err = io.Copy(w, r.Reader)
	if err != nil {
		return info, hdfsToObjectErr(ctx, err, bucket, object, uploadID)
	}

	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

	return info, nil
}

func (n *hdfsObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	name := n.hdfsPathJoin(bucket, object)
	dir := path.Dir(name)
	if dir != "" {
		if err = n.clnt.MkdirAll(dir, os.FileMode(0755)); err != nil {
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	}

	err = n.clnt.Rename(n.hdfsPathJoin(minioMetaTmpBucket, uploadID), name)
	// Object already exists is an error on HDFS
	// remove it and then create it again.
	if os.IsExist(err) {
		if err = n.clnt.Remove(name); err != nil {
			if dir != "" {
				n.deleteObject(n.hdfsPathJoin(bucket), dir)
			}
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
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
