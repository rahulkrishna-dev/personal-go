package s3Migration

import (
	"context"
	"os"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	bucketName          = "seller-hub"
	awsRegion           = "ap-southeast-1"
	maxKeysPerIteration = 20
)

var commonDocumentRootFolders = []string{"iocc", "ownership_documents", "terms_and_condition", "search_insights"}

var logger *zap.Logger

func init() {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			panic("failed to initialize logger: " + err.Error())
		}
	}
}

func IterateS3Keys() {
	args := os.Args
	prefix := args[0]

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion))
	if err != nil {
		logger.Fatal("Unable to load SDK config", zap.Error(err))
	}

	client := s3.NewFromConfig(cfg)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int32(maxKeysPerIteration),
		Prefix:  aws.String(prefix),
	}

	logger.Info("Listing objects in bucket", zap.String("bucket", bucketName))
	pageCount := 0

	for {
		pageCount++
		logger.Info("Retrieving page", zap.Int("page", pageCount))

		output, err := client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			logger.Fatal("Failed to list objects", zap.Error(err))
		}

		keysList := make([]string, 0, maxKeysPerIteration)
		for _, object := range output.Contents {
			keysList = append(keysList, *object.Key)
		}

		err = processKeys(keysList)
		if err != nil {
			logger.Error("Error processing key list", zap.Strings("keys", keysList), zap.Error(err))
			return
		}
		keysList = nil

		if !*output.IsTruncated {
			logger.Info("All objects have been listed.")
			break
		}

		logger.Info("More pages to fetch...")
		input.ContinuationToken = output.NextContinuationToken
	}
}

func processKeys(keyList []string) error {
	documentList := make([]Document, 0, len(keyList))
	for _, key := range keyList {
		payload := buildPayloadFromKey(key)
		if payload != nil {
			documentList = append(documentList, *payload)
		}
	}
	err := callBackFillAPI(&BackFillRequest{
		DocList: documentList,
	})
	if err != nil {
		logger.Error("callBackFillAPI failed", zap.Error(err))
		return err
	}
	logger.Info("Processed key list successfully", zap.Int("count", len(keyList)))
	return nil
}

func buildPayloadFromKey(key string) *Document {
	parts := strings.Split(key, "/")
	rootFolder := parts[0]
	var userIdStr, sellerIdStr *string
	doc := Document{
		S3Key: key,
	}

	if rootFolder == "tax" {
		return nil
	}

	if contains(commonDocumentRootFolders, rootFolder) {
		doc.DocumentType = "COMMON"
		return &doc
	}

	switch rootFolder {
	case "sto_zips":
		doc.DocumentType = "INVENTORY"
		sellerIdStr = &parts[1]
	case "seller":
		doc.DocumentType = parts[3]
		userIdStr = &parts[1]
	}
	if rootFolder == "sto_zips" {
		doc.DocumentType = "INVENTORY"
		sellerIdStr = &parts[1]
	}

	if rootFolder == "seller" {
		doc.DocumentType = parts[3]
		if doc.DocumentType == "active_sellers" {
			doc.DocumentType = "COMMON"
		}
		userIdStr = &parts[1]
	}

	if userIdStr != nil {
		userId, err := strconv.ParseInt(*userIdStr, 64, 10)
		if err != nil {
			logger.Error("Failed to parse user ID", zap.String("userIdStr", *userIdStr), zap.Error(err))
			return nil
		}
		doc.UserId = &userId
	}
	if sellerIdStr != nil {
		sellerId, err := strconv.ParseInt(*sellerIdStr, 64, 10)
		if err != nil {
			logger.Error("Failed to parse seller ID", zap.String("sellerIdStr", *sellerIdStr), zap.Error(err))
			return nil
		}
		doc.SellerId = &sellerId
	}
	return &doc
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}
