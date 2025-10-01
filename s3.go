package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
	"io"
	"io/fs"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidKey = errors.New("invalid key")

func objName(key string) string {
	return strings.TrimLeft(key, "/")
}

func objLockName(key string) string {
	return objName(key) + ".lock"
}

type S3 struct {
	Logger *zap.Logger
	Client *s3sdk.Client
	Bucket string `json:"bucket"`
	Region string `json:"region"`
}

func (s3 S3) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.s3",
		New: func() caddy.Module {
			return new(S3)
		},
	}
}

func init() {
	caddy.RegisterModule(S3{})
}

func (s3 S3) CertMagicStorage() (certmagic.Storage, error) {
	return &s3, nil
}

func (s3 *S3) Provision(ctx caddy.Context) error {
	s3.Logger = ctx.Logger(s3)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(s3.Region),
	)

	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	s3.Client = s3sdk.NewFromConfig(cfg)
	return nil
}

var (
	LockExpiration   = 2 * time.Minute
	LockPollInterval = 1 * time.Second
	LockTimeout      = 15 * time.Second
)

func (s3 *S3) Lock(ctx context.Context, key string) error {
	s3.Logger.Info(fmt.Sprintf("Lock: %v", objName(key)))
	startedAt := time.Now()

	for {
		input := &s3sdk.GetObjectInput{
			Bucket: aws.String(s3.Bucket),
			Key:    aws.String(objLockName(key)),
		}

		result, err := s3.Client.GetObject(ctx, input)
		if err != nil {
			var nsk *types.NoSuchKey
			if errors.As(err, &nsk) {
				return s3.putLockFile(ctx, key)
			}
			continue
		}

		buf, err := io.ReadAll(result.Body)
		_ = result.Body.Close()
		if err != nil {
			continue
		}

		lt, err := time.Parse(time.RFC3339, string(buf))
		if err != nil {
			return s3.putLockFile(ctx, key)
		}
		if lt.Add(LockTimeout).Before(time.Now()) {
			return s3.putLockFile(ctx, key)
		}

		if startedAt.Add(LockTimeout).Before(time.Now()) {
			return errors.New("acquiring lock failed")
		}
		time.Sleep(LockPollInterval)
	}
}

func (s3 *S3) putLockFile(ctx context.Context, key string) error {
	lockData := []byte(time.Now().Format(time.RFC3339))
	r := bytes.NewReader(lockData)

	input := &s3sdk.PutObjectInput{
		Bucket:        aws.String(s3.Bucket),
		Key:           aws.String(objLockName(key)),
		Body:          r,
		ContentLength: aws.Int64(int64(len(lockData))),
	}

	_, err := s3.Client.PutObject(ctx, input)
	return err
}

func (s3 *S3) Unlock(ctx context.Context, key string) error {
	s3.Logger.Info(fmt.Sprintf("Release lock: %v", objName(key)))

	input := &s3sdk.DeleteObjectInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(objLockName(key)),
	}

	_, err := s3.Client.DeleteObject(ctx, input)
	return err
}

func (s3 *S3) Store(ctx context.Context, key string, value []byte) error {
	start := time.Now()
	objName := objName(key)

	if len(value) == 0 {
		return fmt.Errorf("%w: cannot store empty value", ErrInvalidKey)
	}

	s3.Logger.Info("storing object",
		zap.String("key", objName),
		zap.Int("size", len(value)),
		zap.String("bucket", s3.Bucket),
	)

	defer func() {
		s3.Logger.Debug("store completed",
			zap.String("key", objName),
			zap.Duration("duration", time.Since(start)),
		)
	}()

	r := bytes.NewReader(value)

	input := &s3sdk.PutObjectInput{
		Bucket:        aws.String(s3.Bucket),
		Key:           aws.String(objName),
		Body:          r,
		ContentLength: aws.Int64(int64(r.Len())),
	}

	_, err := s3.Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to store key %s: %w", key, err)
	}
	return nil
}

func (s3 *S3) Load(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	objName := objName(key)

	s3.Logger.Info("loading object",
		zap.String("key", objName),
		zap.String("bucket", s3.Bucket),
	)

	defer func() {
		s3.Logger.Debug("load completed",
			zap.String("key", objName),
			zap.Duration("duration", time.Since(start)),
		)
	}()

	input := &s3sdk.GetObjectInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(objName),
	}

	result, err := s3.Client.GetObject(ctx, input)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fs.ErrNotExist
		}
		return nil, fmt.Errorf("failed to load key %s: %w", key, err)
	}
	defer func() { _ = result.Body.Close() }()

	buf, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read/decrypt data for key %s: %w", key, err)
	}
	return buf, nil
}

func (s3 *S3) Delete(ctx context.Context, key string) error {
	start := time.Now()
	objName := objName(key)

	s3.Logger.Info("deleting object",
		zap.String("key", objName),
		zap.String("bucket", s3.Bucket),
	)

	defer func() {
		s3.Logger.Debug("delete completed",
			zap.String("key", objName),
			zap.Duration("duration", time.Since(start)),
		)
	}()

	input := &s3sdk.DeleteObjectInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(objName),
	}

	_, err := s3.Client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

func (s3 *S3) Exists(ctx context.Context, key string) bool {
	objName := objName(key)

	s3.Logger.Debug("checking object existence",
		zap.String("key", objName),
		zap.String("bucket", s3.Bucket),
	)

	input := &s3sdk.HeadObjectInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(objName),
	}

	_, err := s3.Client.HeadObject(ctx, input)
	exists := err == nil

	s3.Logger.Debug("existence check completed",
		zap.String("key", objName),
		zap.Bool("exists", exists),
	)

	return exists
}

func (s3 *S3) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var keys []string

	input := &s3sdk.ListObjectsV2Input{
		Bucket: aws.String(s3.Bucket),
		Prefix: aws.String(objName("")),
	}

	paginator := s3sdk.NewListObjectsV2Paginator(s3.Client, input)
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}

	return keys, nil
}

func (s3 *S3) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	s3.Logger.Info(fmt.Sprintf("Stat: %v", objName(key)))
	var ki certmagic.KeyInfo

	input := &s3sdk.HeadObjectInput{
		Bucket: aws.String(s3.Bucket),
		Key:    aws.String(objName(key)),
	}

	result, err := s3.Client.HeadObject(ctx, input)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return ki, fs.ErrNotExist
		}
		return ki, err
	}

	ki.Key = key
	ki.Size = aws.ToInt64(result.ContentLength)
	ki.Modified = aws.ToTime(result.LastModified)
	ki.IsTerminal = true
	return ki, nil
}

func parseBool(value string) (bool, error) {
	return strconv.ParseBool(value)
}

func (s3 *S3) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	// for d.Next() { // advances to the next directive
	// 	fmt.Printf("Directive: %s (Nesting=%d)\n", d.Val(), d.Nesting())

	// 	// Print any arguments after the directive name
	// 	for d.NextArg() {
	// 		fmt.Printf("  Arg: %s\n", d.Val())
	// 	}

	// 	// Handle nested blocks if present
	// 	for d.NextBlock(0) {
	// 		fmt.Printf("  Block start at nesting=%d\n", d.Nesting())
	// 		for d.NextArg() {
	// 			fmt.Printf("    Block Arg: %s\n", d.Val())
	// 		}
	// 	}
	// }
	d.Next()

	for d.NextBlock(0) {
		switch d.Val() {
		case "bucket":
			if !d.NextArg() {
				return d.Err("bucket requires a value")
			}
			s3.Bucket = d.Val()

		case "region":
			if !d.NextArg() {
				return d.Err("region requires a value")
			}
			s3.Region = d.Val()

		default:
			return d.Errf("unrecognized directive in storage s3 block: %s", d.Val())
		}
	}

	if s3.Bucket == "" || s3.Region == "" {
		return d.Err("storage s3 requires both 'bucket' and 'region'")
	}

	return nil
}

var (
	_ caddy.Provisioner      = (*S3)(nil)
	_ caddy.StorageConverter = (*S3)(nil)
	_ caddyfile.Unmarshaler  = (*S3)(nil)
)
