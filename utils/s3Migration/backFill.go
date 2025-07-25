package s3Migration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"go.uber.org/zap"
)

const host = "http://localhost:8080"

type BackFillRequest struct {
	DocList []Document `json:"document_list"`
}

type Document struct {
	UserId       *int64 `json:"user_id,omitempty"`
	SellerId     *int64 `json:"seller_id,omitempty"`
	DocumentType string `json:"document_type"`
	S3Key        string `json:"s3_key"`
}

func callBackFillAPI(req *BackFillRequest) error {
	url := fmt.Sprintf("%s/seller-hub/internal/api/backfill_seller_document", host)
	payload, err := json.Marshal(req)
	if err != nil {
		logger.Error("Failed to marshal BackFillRequest", zap.Error(err))
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		logger.Error("Failed to make POST request", zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode > http.StatusOK {
		logger.Error("API call failed", zap.String("status", resp.Status))
		return fmt.Errorf("API call failed with status: %s", resp.Status)
	}

	logger.Info("BackFill API call successful", zap.String("url", url))
	return nil
}
