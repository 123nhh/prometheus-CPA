package cpa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client CPA API客户端
type Client struct {
	baseURL       string
	managementKey string
	httpClient    *http.Client
}

// NewClient 创建CPA客户端
func NewClient(baseURL, managementKey string) *Client {
	return &Client{
		baseURL:       strings.TrimSuffix(baseURL, "/"),
		managementKey: managementKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// AuthURLResponse 获取OAuth链接的响应
type AuthURLResponse struct {
	Status string `json:"status"`
	URL    string `json:"url"`
	State  string `json:"state"`
	Error  string `json:"error,omitempty"`
}

// AuthStatusResponse 授权状态响应
type AuthStatusResponse struct {
	Status   string `json:"status"`
	State    string `json:"state,omitempty"`
	Provider string `json:"provider,omitempty"`
	Message  string `json:"message,omitempty"`
	Error    string `json:"error,omitempty"`
}

// OAuthCallbackRequest 提交OAuth回调请求
type OAuthCallbackRequest struct {
	Provider    string `json:"provider"`
	RedirectURL string `json:"redirect_url"`
	Code        string `json:"code"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
}

// AuthFile 认证文件信息
type AuthFile struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Provider      string `json:"provider"`
	Email         string `json:"email"`
	Status        string `json:"status"`
	StatusMessage string `json:"status_message"`
	Disabled      bool   `json:"disabled"`
	Unavailable   bool   `json:"unavailable"`
	RuntimeOnly   bool   `json:"runtime_only"`
	Source        string `json:"source"`
	Path          string `json:"path,omitempty"`
	Size          int64  `json:"size,omitempty"`
	ModTime       string `json:"modtime,omitempty"`
	AccountType   string `json:"account_type,omitempty"`
	Account       string `json:"account,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
	LastRefresh   string `json:"last_refresh,omitempty"`
}

// AuthFilesResponse 认证文件列表响应
type AuthFilesResponse struct {
	Files []AuthFile `json:"files"`
}

// GetAntigravityAuthURL 获取Antigravity OAuth链接
func (c *Client) GetAntigravityAuthURL(ctx context.Context) (*AuthURLResponse, error) {
	return c.getAuthURL(ctx, "/v0/management/antigravity-auth-url")
}

// GetGeminiCLIAuthURL 获取Gemini CLI OAuth链接
func (c *Client) GetGeminiCLIAuthURL(ctx context.Context) (*AuthURLResponse, error) {
	return c.getAuthURL(ctx, "/v0/management/gemini-cli-auth-url")
}

// GetCodexAuthURL 获取Codex OAuth链接
func (c *Client) GetCodexAuthURL(ctx context.Context) (*AuthURLResponse, error) {
	return c.getAuthURL(ctx, "/v0/management/codex-auth-url")
}

// IFlowAuthResponse iFlow Cookie登录响应
type IFlowAuthResponse struct {
	Status    string `json:"status"`
	SavedPath string `json:"saved_path"`
	Email     string `json:"email"`
	Expired   string `json:"expired"`
	Type      string `json:"type"`
	Error     string `json:"error,omitempty"`
}

// SubmitIFlowCookie 提交iFlow Cookie进行登录
func (c *Client) SubmitIFlowCookie(ctx context.Context, cookie string) (*IFlowAuthResponse, error) {
	body := map[string]string{"cookie": cookie}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v0/management/iflow-auth-url", strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.managementKey)
	req.Header.Set("X-Management-Key", c.managementKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	var result IFlowAuthResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK || result.Status != "ok" {
		errMsg := result.Error
		if errMsg == "" {
			errMsg = string(respBody)
		}
		return nil, fmt.Errorf("iFlow auth failed: %s", errMsg)
	}

	return &result, nil
}

func (c *Client) getAuthURL(ctx context.Context, endpoint string) (*AuthURLResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.managementKey)
	req.Header.Set("X-Management-Key", c.managementKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result AuthURLResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response failed: %w", err)
	}

	return &result, nil
}

// GetAuthStatus 获取授权状态
func (c *Client) GetAuthStatus(ctx context.Context, state string) (*AuthStatusResponse, error) {
	statusURL, err := url.Parse(c.baseURL + "/v0/management/get-auth-status")
	if err != nil {
		return nil, fmt.Errorf("parse request URL failed: %w", err)
	}
	query := statusURL.Query()
	query.Set("state", state)
	statusURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", statusURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.managementKey)
	req.Header.Set("X-Management-Key", c.managementKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	var result AuthStatusResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response failed: %w", err)
	}

	return &result, nil
}

// SubmitOAuthCallback 提交OAuth回调
func (c *Client) SubmitOAuthCallback(ctx context.Context, req *OAuthCallbackRequest) error {
	jsonBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request failed: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v0/management/oauth-callback", strings.NewReader(string(jsonBody)))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+c.managementKey)
	httpReq.Header.Set("X-Management-Key", c.managementKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// WaitForAuthComplete 等待授权完成
func (c *Client) WaitForAuthComplete(ctx context.Context, state, provider string, timeout time.Duration) (*AuthStatusResponse, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		status, err := c.GetAuthStatus(ctx, state)
		if err != nil {
			// 可能是网络问题，继续重试
			time.Sleep(2 * time.Second)
			continue
		}

		// 检查状态
		if status.Status == "error" {
			return status, fmt.Errorf("auth failed: %s", status.Error)
		}

		// 如果state不存在了，说明授权已完成（CPA会在成功后删除session）
		if status.Error == "unknown or expired state" {
			return &AuthStatusResponse{
				Status:   "completed",
				Provider: provider,
			}, nil
		}

		// 如果有错误消息，说明授权失败
		if status.Message != "" {
			return status, fmt.Errorf("auth failed: %s", status.Message)
		}

		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("auth timeout after %v", timeout)
}

// HashState 生成state的哈希（用于去重检查）
func HashState(credType, state string) string {
	h := sha256.New()
	h.Write([]byte(credType + ":" + state))
	return hex.EncodeToString(h.Sum(nil))
}

// GetAuthFiles 获取CPA中的所有认证文件
func (c *Client) GetAuthFiles(ctx context.Context) (*AuthFilesResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/v0/management/auth-files", nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.managementKey)
	req.Header.Set("X-Management-Key", c.managementKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result AuthFilesResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response failed: %w", err)
	}

	return &result, nil
}

// GetExistingEmails 获取CPA中指定provider的所有邮箱列表
func (c *Client) GetExistingEmails(ctx context.Context, provider string) (map[string]bool, error) {
	authFiles, err := c.GetAuthFiles(ctx)
	if err != nil {
		return nil, err
	}

	emails := make(map[string]bool)
	for _, f := range authFiles.Files {
		if f.Email != "" && (provider == "" || f.Provider == provider) {
			emails[f.Email] = true
		}
	}
	return emails, nil
}

// CheckEmailExists 检查指定邮箱是否已存在于CPA凭证中
func (c *Client) CheckEmailExists(ctx context.Context, email, provider string) (bool, error) {
	emails, err := c.GetExistingEmails(ctx, provider)
	if err != nil {
		return false, err
	}
	return emails[email], nil
}
