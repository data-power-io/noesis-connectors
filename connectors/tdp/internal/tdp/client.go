package tdp

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

type Client struct {
	s3Client *s3.Client
	bucket   string
	tdpPath  string
	logger   *zap.Logger
}

func NewClient(cfg map[string]string, logger *zap.Logger) (*Client, error) {
	// Parse SSL setting
	useSSL := true
	if sslStr := cfg["use_ssl"]; sslStr != "" {
		if parsed, err := strconv.ParseBool(sslStr); err == nil {
			useSSL = parsed
		}
	}

	// Build endpoint URL
	endpoint := cfg["endpoint"]
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		if useSSL {
			endpoint = "https://" + endpoint
		} else {
			endpoint = "http://" + endpoint
		}
	}

	region := cfg["region"]
	if region == "" {
		region = "us-east-1"
	}

	// Load AWS config with static credentials and custom endpoint
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg["access_key_id"],
			cfg["secret_access_key"],
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint and path-style addressing for MinIO compatibility
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	client := &Client{
		s3Client: s3Client,
		bucket:   cfg["bucket"],
		tdpPath:  cfg["tdp_path"],
		logger:   logger,
	}

	return client, nil
}

func (c *Client) Close() {
	// S3 client doesn't require explicit closing
}

func (c *Client) Ping(ctx context.Context) error {
	// Try to list objects with max 1 result to verify connectivity
	_, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(c.tdpPath),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to S3: %w", err)
	}
	return nil
}

// GetBOMManifest reads and parses the BOM manifest CSV file
func (c *Client) GetBOMManifest(ctx context.Context) ([]BOMRecord, error) {
	manifestPath := path.Join(c.tdpPath, "BOM_Manifest_With_Docs.csv")

	// Get the object from S3
	result, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(manifestPath),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get BOM manifest: %w", err)
	}
	defer result.Body.Close()

	// Parse CSV
	reader := csv.NewReader(result.Body)

	// Read header row
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Validate header has expected columns
	if len(header) < 13 {
		return nil, fmt.Errorf("expected at least 13 columns in BOM manifest, got %d", len(header))
	}

	var records []BOMRecord
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}

		record, err := c.parseBOMRow(row)
		if err != nil {
			c.logger.Warn("Failed to parse BOM row", zap.Error(err))
			continue
		}
		records = append(records, record)
	}

	return records, nil
}

func (c *Client) parseBOMRow(row []string) (BOMRecord, error) {
	if len(row) < 13 {
		return BOMRecord{}, fmt.Errorf("row has %d columns, expected at least 13", len(row))
	}

	level, _ := strconv.Atoi(strings.TrimSpace(row[0]))
	qty, _ := strconv.Atoi(strings.TrimSpace(row[5]))
	weight, _ := strconv.ParseFloat(strings.TrimSpace(row[8]), 64)

	return BOMRecord{
		Level:          level,
		ParentPN:       strings.TrimSpace(row[1]),
		PartNumber:     strings.TrimSpace(row[2]),
		Revision:       strings.TrimSpace(row[3]),
		Nomenclature:   strings.TrimSpace(row[4]),
		Qty:            qty,
		Type:           strings.TrimSpace(row[6]),
		Material:       strings.TrimSpace(row[7]),
		WeightKg:       weight,
		CAGECode:       strings.TrimSpace(row[9]),
		CADFile:        strings.TrimSpace(row[10]),
		LinkedDocument: strings.TrimSpace(row[11]),
		DocumentType:   strings.TrimSpace(row[12]),
	}, nil
}

// ListCADModels lists all STEP files in the 3D_Models directory
func (c *Client) ListCADModels(ctx context.Context) ([]FileMetadata, error) {
	return c.listFilesInDirectory(ctx, "3D_Models", []string{".step", ".stp", ".STEP", ".STP"})
}

// ListDocuments lists all PDF files in the Documents directory
func (c *Client) ListDocuments(ctx context.Context) ([]FileMetadata, error) {
	return c.listFilesInDirectory(ctx, "Documents", []string{".pdf", ".PDF"})
}

func (c *Client) listFilesInDirectory(ctx context.Context, subdir string, extensions []string) ([]FileMetadata, error) {
	prefix := path.Join(c.tdpPath, subdir) + "/"

	var files []FileMetadata
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			ext := path.Ext(key)

			// Filter by extension
			if !c.hasExtension(ext, extensions) {
				continue
			}

			var modifiedTime int64
			if obj.LastModified != nil {
				modifiedTime = obj.LastModified.UnixMicro()
			}

			files = append(files, FileMetadata{
				Path:         key,
				Name:         path.Base(key),
				Size:         aws.ToInt64(obj.Size),
				ModifiedTime: modifiedTime,
				Extension:    ext,
			})
		}
	}

	return files, nil
}

func (c *Client) hasExtension(ext string, allowed []string) bool {
	for _, a := range allowed {
		if strings.EqualFold(ext, a) {
			return true
		}
	}
	return false
}

// GetRowCounts returns estimated row counts for each entity
func (c *Client) GetRowCounts(ctx context.Context) (map[EntityType]int64, error) {
	counts := make(map[EntityType]int64)

	// Count BOM records
	bomRecords, err := c.GetBOMManifest(ctx)
	if err != nil {
		return nil, err
	}
	counts[EntityBOMManifest] = int64(len(bomRecords))

	// Count CAD files
	cadFiles, err := c.ListCADModels(ctx)
	if err != nil {
		return nil, err
	}
	counts[EntityCADModels] = int64(len(cadFiles))

	// Count documents
	docs, err := c.ListDocuments(ctx)
	if err != nil {
		return nil, err
	}
	counts[EntityDocuments] = int64(len(docs))

	return counts, nil
}
