package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type AnalyticsClient struct {
	baseURL string
	client  *http.Client
}

func NewAnalyticsClient(baseURL string) *AnalyticsClient {
	return &AnalyticsClient{baseURL: baseURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (c *AnalyticsClient) GetStatistics(ctx context.Context) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/statistics", nil)
	if err != nil {
		return nil, fmt.Errorf("Error of creating new http request: %w", err)
	}

	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error http request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("analytics service returned status: %d", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error http request: %w", err)
	}

	return body, nil
}
