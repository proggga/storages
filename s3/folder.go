package s3

import (
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	NotFoundAWSErrorCode  = "NotFound"
	NoSuchKeyAWSErrorCode = "NoSuchKey"

	EndpointSetting          = "AWS_ENDPOINT"
	RegionSetting            = "AWS_REGION"
	ForcePathStyleSetting    = "AWS_S3_FORCE_PATH_STYLE"
	AccessKeyIdSetting       = "AWS_ACCESS_KEY_ID"
	AccessKeySetting         = "AWS_ACCESS_KEY"
	SecretAccessKeySetting   = "AWS_SECRET_ACCESS_KEY"
	SecretKeySetting         = "AWS_SECRET_KEY"
	SessionTokenSetting      = "AWS_SESSION_TOKEN"
	SseSetting               = "S3_SSE"
	SseCSetting              = "S3_SSE_C"
	SseKmsIdSetting          = "S3_SSE_KMS_ID"
	StorageClassSetting      = "S3_STORAGE_CLASS"
	UploadConcurrencySetting = "UPLOAD_CONCURRENCY"
	s3CertFile               = "S3_CA_CERT_FILE"
	MaxPartSize              = "S3_MAX_PART_SIZE"
	EndpointSourceSetting    = "S3_ENDPOINT_SOURCE"
	EndpointPortSetting      = "S3_ENDPOINT_PORT"
	LogLevel                 = "S3_LOG_LEVEL"
	UseListObjectsV1         = "S3_USE_LIST_OBJECTS_V1"
	RangeBatchSize           = "S3_RANGE_BATCH_SIZE"
)

var (
	// MaxRetries limit upload and download retries during interaction with S3
	MaxRetries  = 15
	SettingList = []string{
		EndpointPortSetting,
		EndpointSetting,
		EndpointSourceSetting,
		RegionSetting,
		ForcePathStyleSetting,
		AccessKeyIdSetting,
		AccessKeySetting,
		SecretAccessKeySetting,
		SecretKeySetting,
		SessionTokenSetting,
		SseSetting,
		SseCSetting,
		SseKmsIdSetting,
		StorageClassSetting,
		UploadConcurrencySetting,
		s3CertFile,
		MaxPartSize,
		UseListObjectsV1,
		RangeBatchSize,
	}
)

func getFirstSettingOf(settings map[string]string, keys []string) string {
	for _, key := range keys {
		if value, ok := settings[key]; ok {
			return value
		}
	}
	return ""
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "S3", format, args...)
}

func NewConfiguringError(settingName string) storage.Error {
	return NewFolderError(errors.New("Configuring error"),
		"%s setting is not set", settingName)
}

type Folder struct {
	uploader Uploader
	S3API    s3iface.S3API
	Bucket   *string
	Path     string
	settings map[string]string

	useListObjectsV1 bool
}

func NewFolder(uploader Uploader, s3API s3iface.S3API, bucket, path string, useListObjectsV1 bool) *Folder {
	return &Folder{
		uploader:         uploader,
		S3API:            s3API,
		Bucket:           aws.String(bucket),
		Path:             storage.AddDelimiterToPath(path),
		useListObjectsV1: useListObjectsV1,
	}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	bucket, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 path")
	}
	sess, err := createSession(bucket, settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new session")
	}
	client := s3.New(sess)
	uploader, err := configureUploader(client, settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 uploader")
	}
	useListObjectsV1 := false
	if strUseListObjectsV1, ok := settings[UseListObjectsV1]; ok {
		useListObjectsV1, err = strconv.ParseBool(strUseListObjectsV1)
		if err != nil {
			return nil, NewFolderError(err, "Invalid s3 list objects version setting")
		}
	}

	folder := NewFolder(*uploader, client, bucket, path, useListObjectsV1)
	folder.settings = settings
	fmt.Printf("SETTING %+v\n", settings)
	fmt.Printf("SETTING_2 %+v\n", settings)

	return folder, nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	objectPath := folder.Path + objectRelativePath
	stopSentinelObjectInput := &s3.HeadObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	_, err := folder.S3API.HeadObject(stopSentinelObjectInput)
	if err != nil {
		if isAwsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to check s3 object '%s' existance", objectPath)
	}
	return true, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	return folder.uploader.upload(*folder.Bucket, folder.Path+name, content)
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return errors.New("object does not exist")
		} else {
			return err
		}
	}
	source := path.Join(*folder.Bucket, folder.Path, srcPath)
	dst := path.Join(folder.Path, dstPath)
	input := &s3.CopyObjectInput{CopySource: &source, Bucket: folder.Bucket, Key: &dst}
	_, err := folder.S3API.CopyObject(input)
	if err != nil {
		return err
	}
	return nil
}

type s3Reader struct {
	ob            *s3.GetObjectOutput
	folder        *Folder
	contentLength int64
	objectPath    string
	storageCursor int64
	cache         []byte
	cacheCursor   int64
	cacheSize     int64
}

func (reader *s3Reader) getObjectRange(from, to int64) (*s3.GetObjectOutput, error) {
	bytesRange := fmt.Sprintf("bytes=%d-%d", from, to)
	input := &s3.GetObjectInput{
		Bucket: reader.folder.Bucket,
		Key:    aws.String(reader.objectPath),
		Range:  aws.String(bytesRange),
	}

	fmt.Printf("GET RANGE %s\n", bytesRange)

	return reader.folder.S3API.GetObject(input)
}

func (reader *s3Reader) ReadToCache() (err error) {
	// we try to take one more bit, I'm trying to avoid EOF in Range requests
	end := reader.storageCursor + reader.cacheSize + 1
	if end >= reader.contentLength {
		end = reader.contentLength
	}

	from := reader.storageCursor
	to := end - 1
	object, err := reader.getObjectRange(from, to)
	if err != nil {
		fmt.Printf("GET GetObject returned error: %s\n", err)
		return err
	}
	defer func() { _ = object.Body.Close() }()

	nval, err1 := object.Body.Read(reader.cache)
	if err1 != nil && err1 != io.EOF {
		return err1
	}
	if nval > 0 {
		fmt.Printf("move cursor %d + %d\n", reader.storageCursor, nval)
		reader.storageCursor += int64(nval)
	}
	return err1
}

func (reader *s3Reader) copyBuf(p []byte, from int) int {
	size:= int64(len(p[from:]))
	readto := reader.cacheCursor + size
	if readto > reader.cacheSize {
		readto = reader.cacheSize
	}

	fmt.Printf("copy %d\n", len(p))
	n := copy(p[from:], reader.cache[reader.cacheCursor:readto])
	fmt.Printf("from %d [%d] -- [%d | %d]\n", from, len(p), reader.cacheCursor, readto)
	reader.cacheCursor = readto

	if reader.cacheCursor >= reader.cacheSize {
		reader.cacheCursor = 0
		reader.cache = nil
		fmt.Printf("reset cache\n")
	}
	return n
}

func (reader *s3Reader) refillCache() error {
	var cacheError error = nil
	if reader.cache == nil {
		fmt.Printf("Refill cache\n")
		reader.cache = make([]byte, reader.cacheSize)
		cacheError = reader.ReadToCache()
		if cacheError != nil && cacheError != io.EOF {
			fmt.Printf("return error %s", cacheError)
			return cacheError
		}
	}
	return cacheError
}

func (reader *s3Reader) Read(p []byte) (n int, err error) {
	fmt.Printf("HELLO READ %d\n", len(p))
	totalRead := 0
	err = reader.refillCache()
	if err != nil && err != io.EOF {
		return 0, err
	}

	if err == io.EOF {
		fmt.Printf("EOF EOF EOF\n")
	}

	i := reader.copyBuf(p, 0)
	totalRead += i
	for totalRead < len(p)  {
		fmt.Printf("[total] %d < %d : %d\n", totalRead, len(p), i)
		err = reader.refillCache()
		if err == io.EOF {
			fmt.Printf("EOF EOF EOF\n")
		}
		if err != nil && err != io.EOF {
			return totalRead, err
		}

		i := reader.copyBuf(p, totalRead)
		totalRead += i

	}
	var er error = nil
	//totalRead, er := reader.ob.Body.Read(p)
	l := string(p)
	if len(l) > 40 {
		l = l[0:40] + "..."
	}
	fmt.Printf("copy buf [%d] %d: %s\n", len(p), totalRead, er)
	fmt.Printf("copy buf detail [%d] %v %d: %s\n", len(p), p, totalRead, er)
	return totalRead, nil
	//return x, y
}

func (reader *s3Reader) Close() (err error) {
	return nil
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectPath := folder.Path + objectRelativePath
	input := &s3.GetObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	object, err := folder.S3API.GetObject(input)
	if err != nil {
		if isAwsNotExist(err) {
			return nil, storage.NewObjectNotFoundError(objectPath)
		}
		return nil, errors.Wrapf(err, "failed to read object: '%s' from S3", objectPath)
	}

	rangeBatchSize := int64(1024)
	fmt.Printf("setting %v\n", folder.settings)
	if batchSize, ok := folder.settings[RangeBatchSize]; ok {
		if iBatchSize, err := strconv.Atoi(batchSize); err == nil {
			rangeBatchSize = int64(iBatchSize)
		}
	}
	return &s3Reader{objectPath: objectPath, ob: object, cacheSize: rangeBatchSize, contentLength: *object.ContentLength, folder: folder}, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	subFolder := NewFolder(folder.uploader, folder.S3API, *folder.Bucket,
		storage.JoinPath(folder.Path, subFolderRelativePath)+"/", folder.useListObjectsV1)
	subFolder.settings = folder.settings
	return subFolder
}

func (folder *Folder) GetPath() string {
	return folder.Path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	listFunc := func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object) {
		for _, prefix := range commonPrefixes {
			subFolder := NewFolder(folder.uploader, folder.S3API, *folder.Bucket,
				*prefix.Prefix, folder.useListObjectsV1)
			subFolder.settings = folder.settings
			subFolders = append(subFolders, subFolder)
		}
		for _, object := range contents {
			// Some storages return root tar_partitions folder as a Key.
			// We do not want to fail restoration due to this fact.
			// Keep in mind that skipping files is very dangerous and any decision here must be weighted.
			if *object.Key == folder.Path {
				continue
			}
			objectRelativePath := strings.TrimPrefix(*object.Key, folder.Path)
			objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified, *object.Size))
		}
	}

	prefix := aws.String(folder.Path)
	delimiter := aws.String("/")
	if folder.useListObjectsV1 {
		err = folder.listObjectsPagesV1(prefix, delimiter, listFunc)
	} else {
		err = folder.listObjectsPagesV2(prefix, delimiter, listFunc)
	}

	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.Path)
	}
	return objects, subFolders, nil
}

func (folder *Folder) listObjectsPagesV1(prefix *string, delimiter *string,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) error {
	s3Objects := &s3.ListObjectsInput{
		Bucket:    folder.Bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	return folder.S3API.ListObjectsPages(s3Objects, func(files *s3.ListObjectsOutput, lastPage bool) bool {
		listFunc(files.CommonPrefixes, files.Contents)
		return true
	})
}

func (folder *Folder) listObjectsPagesV2(prefix *string, delimiter *string,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) error {
	s3Objects := &s3.ListObjectsV2Input{
		Bucket:    folder.Bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	return folder.S3API.ListObjectsV2Pages(s3Objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		listFunc(files.CommonPrefixes, files.Contents)
		return true
	})
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	parts := partitionStrings(objectRelativePaths, 1000)
	for _, part := range parts {
		input := &s3.DeleteObjectsInput{Bucket: folder.Bucket, Delete: &s3.Delete{
			Objects: folder.partitionToObjects(part),
		}}
		_, err := folder.S3API.DeleteObjects(input)
		if err != nil {
			return errors.Wrapf(err, "failed to delete s3 object: '%s'", part)
		}
	}
	return nil
}

func (folder *Folder) partitionToObjects(keys []string) []*s3.ObjectIdentifier {
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for id, key := range keys {
		objects[id] = &s3.ObjectIdentifier{Key: aws.String(folder.Path + key)}
	}
	return objects
}

func isAwsNotExist(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == NotFoundAWSErrorCode || awsErr.Code() == NoSuchKeyAWSErrorCode {
			return true
		}
	}
	return false
}
