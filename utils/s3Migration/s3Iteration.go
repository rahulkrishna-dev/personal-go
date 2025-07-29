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
	bucketName          = "blinkit-seller-stage"
	awsRegion           = "us-west-2"
	maxKeysPerIteration = 20
)

var commonDocumentRootFolders = []string{"iocc", "ownership_documents", "terms_and_condition", "search_insights"}

var documentTypeMap = map[string]string{
	"iocc":                "IOCC",
	"ownership_documents": "OWNERSHIP",
	"terms_and_condition": "T_AND_C",
	"search_insights":     "SEARCH_INSIGHTS",
	"sto_zips":            "STO",
	"payout":              "PAYOUT",
	"GST":                 "GST",
	"PAN":                 "PAN",
	"FSSAI":               "FSSAI",
	"Brand_Authorization": "Brand_Authorization",
	"Brand_Trademark":     "Brand_Trademark",
	"Brand_Logo":          "Brand_Logo",
	"ARN_Certificate":     "ARN_Certificate",
	"Digital_Signature":   "Digital_Signature",
	"CIN":                 "CIN",
	"MSME":                "MSME",
	"Cancelled_Cheque":    "Cancelled_Cheque",
	"noc":                 "NOC",
	"serviceability":      "SERVICEABILITY",
	"soa":                 "SOA",
	"availability":        "AVAILABILITY",
	"daily-ageing":        "DAILY_AGEING",
	"movement_invoice":    "MOVEMENT_INVOICE",
	"sales-performance":   "SALES_PERFORMANCE",
}

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
	prefix = "search_insights/"

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
	if len(parts) < 1 {
		logger.Error("Key split resulted in empty parts", zap.String("key", key))
		return nil
	}
	rootFolder := parts[0]
	var userIdStr, sellerIdStr *string
	doc := Document{
		S3Key: key,
	}

	if rootFolder == "tax" {
		return nil
	}

	if contains(commonDocumentRootFolders, rootFolder) {
		doc.DocumentType = documentTypeMap[rootFolder]
		return &doc
	}

	switch rootFolder {
	case "sto_zips":
		if len(parts) < 2 { //done
			logger.Error("sto_zips key missing sellerId", zap.String("key", key))
			return nil
		}
		doc.DocumentType = documentTypeMap[rootFolder]
		sellerIdStr = &parts[1]
	case "seller":
		if len(parts) < 4 {
			logger.Error("seller key missing expected indices", zap.String("key", key))
			return nil
		}
		docType := parts[3]
		if transformedDocType, ok := documentTypeMap[docType]; ok {
			docType = transformedDocType
		}
		if docType == "active_sellers" {
			docType = "INTERNAL_DASHBOARD"
		}
		if docType == "inventory" {
			if len(parts) == 7 && parts[6] == "InventoryData.xlsx" {
				docType = "SOH_SHEET"
			}
			if len(parts) == 6 && strings.HasPrefix(parts[5], "SELLER_BULK_SHIPMENT") {
				docType = "BULK_STO"
			}
		}
		doc.DocumentType = docType
		userIdStr = &parts[1]
	case "reports":
		if len(parts) < 4 {
			logger.Error("reports key missing expected indices", zap.String("key", key))
			return nil
		}
		if parts[1] == "sales-performance" || parts[1] == "availability" || parts[1] == "soa" {
			docType := parts[1]
			sellerIdStr = &parts[2]
			doc.DocumentType = documentTypeMap[docType]
		}
		if parts[1] == "movement_invoice" || parts[1] == "daily-ageing" {
			docType := parts[1]
			sellerIdStr = &parts[3]
			doc.DocumentType = documentTypeMap[docType]
		}
	}

	if userIdStr != nil {
		userId, err := strconv.ParseInt(*userIdStr, 10, 64)
		if err != nil {
			logger.Error("Failed to parse user ID", zap.String("userIdStr", *userIdStr), zap.Error(err))
			return nil
		}
		doc.UserId = &userId
	}
	if sellerIdStr != nil {
		sellerId, err := strconv.ParseInt(*sellerIdStr, 10, 64)
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
