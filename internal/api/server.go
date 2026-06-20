package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donation-station/donation-station/internal/callback"
	"github.com/donation-station/donation-station/internal/cdk"
	"github.com/donation-station/donation-station/internal/config"
	"github.com/donation-station/donation-station/internal/cpa"
	"github.com/donation-station/donation-station/internal/database"
	"github.com/gin-gonic/gin"
)

// PendingAuth 等待中的授权
type PendingAuth struct {
	State             string
	Type              string
	GroupID           *int64 // CDK分组ID
	CreatedAt         time.Time
	ExistingEmails    map[string]bool // 提交回调前已存在的邮箱列表
	AuthFilesProvider string          // auth-files API 中使用的 provider 名称
}

// Server API服务器
type Server struct {
	cfg          *config.Config
	db           *database.DB
	router       *gin.Engine
	cpaClient    *cpa.Client
	cdkGen       *cdk.Generator
	notifier     *callback.Notifier
	pendingAuths map[string]*PendingAuth
	pendingMu    sync.RWMutex
}

// NewServer 创建服务器
func NewServer(cfg *config.Config, db *database.DB) *Server {
	gin.SetMode(gin.ReleaseMode)

	s := &Server{
		cfg:          cfg,
		db:           db,
		router:       gin.Default(),
		cpaClient:    cpa.NewClient(cfg.CPABaseURL, cfg.CPAManagementKey),
		cdkGen:       cdk.NewGenerator(cfg.CDKPrefix),
		notifier:     callback.NewNotifier(cfg.CallbackURL, cfg.CallbackSecret),
		pendingAuths: make(map[string]*PendingAuth),
	}

	s.setupRoutes()
	go s.cleanupExpiredAuths()
	return s
}

// cleanupExpiredAuths 清理过期的授权
func (s *Server) cleanupExpiredAuths() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.pendingMu.Lock()
		now := time.Now()
		for state, auth := range s.pendingAuths {
			if now.Sub(auth.CreatedAt) > 10*time.Minute {
				delete(s.pendingAuths, state)
			}
		}
		s.pendingMu.Unlock()
	}
}

// getOrCreateCDK 获取可用CDK，如果CDK用完则自动创建"待兑换"分组并生成新CDK
func (s *Server) getOrCreateCDK(groupID *int64) (*database.CDK, error) {
	// 先尝试获取现有的可用CDK
	cdkRecord, err := s.db.GetAvailableCDK(groupID)
	if err != nil {
		return nil, err
	}

	// 如果有可用的CDK，直接返回
	if cdkRecord != nil {
		return cdkRecord, nil
	}

	// CDK已用完，查找或创建"待兑换"分组
	groups, err := s.db.ListCDKGroups()
	if err != nil {
		return nil, fmt.Errorf("列出分组失败: %w", err)
	}

	var pendingGroupID *int64
	for _, g := range groups {
		if g.Name == "待兑换" {
			pendingGroupID = &g.ID
			break
		}
	}

	// 如果没有"待兑换"分组，创建一个
	if pendingGroupID == nil {
		newGroup, err := s.db.CreateCDKGroup("待兑换", "CDK池耗尽时自动创建的分组，需要用户自行兑换")
		if err != nil {
			return nil, fmt.Errorf("创建待兑换分组失败: %w", err)
		}
		pendingGroupID = &newGroup.ID
	}

	// 生成新的CDK
	newCode, err := s.cdkGen.Generate()
	if err != nil {
		return nil, fmt.Errorf("生成CDK失败: %w", err)
	}

	// 添加到待兑换分组
	if err := s.db.AddCDK(newCode, pendingGroupID); err != nil {
		return nil, fmt.Errorf("添加CDK失败: %w", err)
	}

	// 再次获取刚创建的CDK
	cdkRecord, err = s.db.GetAvailableCDK(pendingGroupID)
	if err != nil {
		return nil, err
	}

	return cdkRecord, nil
}

// setupRoutes 设置路由
func (s *Server) setupRoutes() {
	// 静态文件
	s.router.Static("/static", "./static")

	// 页面路由
	s.router.GET("/", s.indexHandler)
	s.router.GET("/admin", s.adminPageHandler)
	s.router.GET("/success", s.successPageHandler)
	s.router.GET("/error", s.errorPageHandler)
	s.router.GET("/waiting", s.waitingPageHandler)

	// API 路由
	api := s.router.Group("/api")
	{
		// OAuth 流程
		api.POST("/auth/start", s.authStartHandler)
		api.GET("/auth/status", s.authStatusHandler)
		api.POST("/auth/callback", s.authCallbackHandler) // 提交回调URL
		api.POST("/auth/complete", s.authCompleteHandler)
		api.POST("/auth/iflow", s.iflowAuthHandler) // iFlow Cookie登录

		// 公开API
		api.GET("/site-config", s.getSiteConfigHandler)
		api.GET("/channels", s.getChannelsHandler)           // 获取渠道配置
		api.GET("/cdk-groups", s.listCDKGroupsPublicHandler) // 公开的CDK分组列表
		api.GET("/public-stats", s.publicStatsHandler)       // 公开统计（仅凭证数量）

		// 管理员API (需要认证)
		admin := api.Group("/admin")
		admin.Use(s.adminAuthMiddleware())
		{
			admin.GET("/stats", s.statsHandler)
			admin.GET("/credentials", s.listCredentialsHandler)
			admin.DELETE("/credentials/:id", s.deleteCredentialHandler)
			admin.GET("/cdks", s.listCDKsHandler)
			admin.POST("/site-config", s.setSiteConfigHandler)

			// CDK管理
			admin.POST("/cdks", s.addCDKHandler)                      // 添加单个CDK
			admin.POST("/cdks/batch", s.batchAddCDKHandler)           // 批量导入CDK
			admin.DELETE("/cdks/:id", s.deleteCDKHandler)             // 删除CDK
			admin.POST("/cdks/batch-delete", s.batchDeleteCDKHandler) // 批量删除CDK

			// CDK分组管理
			admin.GET("/cdk-groups", s.listCDKGroupsHandler)
			admin.POST("/cdk-groups", s.createCDKGroupHandler)
			admin.PUT("/cdk-groups/:id", s.updateCDKGroupHandler)
			admin.DELETE("/cdk-groups/:id", s.deleteCDKGroupHandler)

			// 渠道管理
			admin.POST("/channels", s.setChannelHandler)
		}
	}
}

// Run 运行服务器
func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}

// indexHandler 首页
func (s *Server) indexHandler(c *gin.Context) {
	c.File("./static/index.html")
}

// adminPageHandler 管理页面
func (s *Server) adminPageHandler(c *gin.Context) {
	c.File("./static/admin.html")
}

// successPageHandler 成功页面
func (s *Server) successPageHandler(c *gin.Context) {
	c.File("./static/success.html")
}

// errorPageHandler 错误页面
func (s *Server) errorPageHandler(c *gin.Context) {
	c.File("./static/error.html")
}

// waitingPageHandler 等待页面
func (s *Server) waitingPageHandler(c *gin.Context) {
	c.File("./static/waiting.html")
}

// AuthStartRequest 开始授权请求
type AuthStartRequest struct {
	Type    string `json:"type" binding:"required"` // "antigravity", "gemini_cli" 或 "codex"
	GroupID *int64 `json:"group_id,omitempty"`      // CDK分组ID
}

// AuthStartResponse 开始授权响应
type AuthStartResponse struct {
	Success bool   `json:"success"`
	AuthURL string `json:"auth_url,omitempty"`
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
}

// authStartHandler 开始OAuth授权流程 - 通过CPA获取OAuth链接
func (s *Server) authStartHandler(c *gin.Context) {
	var req AuthStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, AuthStartResponse{
			Success: false,
			Message: "请求参数错误: " + err.Error(),
		})
		return
	}

	// 统一类型格式：gemini-cli -> gemini_cli
	normalizedType := strings.ReplaceAll(req.Type, "-", "_")

	// 验证类型
	if normalizedType != "antigravity" && normalizedType != "gemini_cli" && normalizedType != "codex" {
		c.JSON(http.StatusBadRequest, AuthStartResponse{
			Success: false,
			Message: "不支持的凭证类型",
		})
		return
	}

	// 检查渠道是否启用
	channelEnabled, _ := s.db.GetSiteConfig("channel_" + normalizedType)
	if channelEnabled == "false" {
		c.JSON(http.StatusBadRequest, AuthStartResponse{
			Success: false,
			Message: "该捐赠渠道已关闭",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// 调用CPA获取OAuth链接
	var authResp *cpa.AuthURLResponse
	var err error

	switch normalizedType {
	case "antigravity":
		authResp, err = s.cpaClient.GetAntigravityAuthURL(ctx)
	case "gemini_cli":
		authResp, err = s.cpaClient.GetGeminiCLIAuthURL(ctx)
	case "codex":
		authResp, err = s.cpaClient.GetCodexAuthURL(ctx)
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "获取授权链接失败: " + err.Error(),
		})
		return
	}

	if authResp.Status != "ok" || authResp.URL == "" {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "CPA返回错误: " + authResp.Error,
		})
		return
	}

	// 保存待处理的授权
	s.pendingMu.Lock()
	s.pendingAuths[authResp.State] = &PendingAuth{
		State:     authResp.State,
		Type:      normalizedType,
		GroupID:   req.GroupID,
		CreatedAt: time.Now(),
	}
	s.pendingMu.Unlock()

	c.JSON(http.StatusOK, AuthStartResponse{
		Success: true,
		AuthURL: authResp.URL,
		State:   authResp.State,
	})
}

// AuthStatusRequest 查询授权状态请求
type AuthStatusRequest struct {
	State string `form:"state" binding:"required"`
}

// AuthStatusResponse 授权状态响应
type AuthStatusResponse struct {
	Success   bool   `json:"success"`
	Status    string `json:"status"` // "pending", "completed", "error"
	Message   string `json:"message,omitempty"`
	CDK       string `json:"cdk,omitempty"`
	Email     string `json:"email,omitempty"`
	ProjectID string `json:"project_id,omitempty"`
}

// authStatusHandler 查询授权状态
func (s *Server) authStatusHandler(c *gin.Context) {
	state := c.Query("state")
	if state == "" {
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "缺少state参数",
		})
		return
	}

	// 检查是否是我们跟踪的授权
	s.pendingMu.RLock()
	_, exists := s.pendingAuths[state]
	s.pendingMu.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "授权会话不存在或已过期",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// 查询CPA的授权状态
	status, err := s.cpaClient.GetAuthStatus(ctx, state)
	if err != nil {
		// 如果查询失败，可能是网络问题，返回pending状态
		c.JSON(http.StatusOK, AuthStatusResponse{
			Success: true,
			Status:  "pending",
			Message: "正在等待授权完成...",
		})
		return
	}

	// 如果state不存在了（CPA返回unknown state），说明授权已完成
	if status.Error == "unknown or expired state" {
		c.JSON(http.StatusOK, AuthStatusResponse{
			Success: true,
			Status:  "completed",
			Message: "授权已完成，请点击确认领取CDK",
		})
		return
	}

	// 如果有错误消息
	if status.Message != "" {
		c.JSON(http.StatusOK, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: status.Message,
		})
		return
	}

	// 还在等待中
	c.JSON(http.StatusOK, AuthStatusResponse{
		Success: true,
		Status:  "pending",
		Message: "正在等待授权完成...",
	})
}

// AuthCallbackRequest 提交回调URL请求
type AuthCallbackRequest struct {
	State       string `json:"state" binding:"required"`
	CallbackURL string `json:"callback_url" binding:"required"`
}

// authCallbackHandler 处理用户提交的回调URL
func (s *Server) authCallbackHandler(c *gin.Context) {
	var req AuthCallbackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "请求参数错误: " + err.Error(),
		})
		return
	}

	// 检查是否是我们跟踪的授权
	s.pendingMu.RLock()
	pending, exists := s.pendingAuths[req.State]
	s.pendingMu.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "授权会话不存在或已过期",
		})
		return
	}

	// 解析回调URL获取code和state
	parsedURL, err := parseCallbackURL(req.CallbackURL)
	if err != nil {
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "无效的回调URL: " + err.Error(),
		})
		return
	}

	// 验证state匹配
	if parsedURL.State != req.State {
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "回调URL中的state不匹配",
		})
		return
	}

	// 检查是否有错误
	if parsedURL.Error != "" {
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "授权失败: " + parsedURL.Error,
		})
		return
	}

	// 确定provider名称
	// CPA oauth-callback API 接受的 provider 名称
	oauthProvider := "antigravity"
	// CPA auth-files API 返回的 provider 名称
	authFilesProvider := "antigravity"
	switch pending.Type {
	case "gemini_cli":
		oauthProvider = "gemini"         // oauth-callback API 用 "gemini"
		authFilesProvider = "gemini-cli" // auth-files 返回 "gemini-cli"
	case "codex":
		oauthProvider = "codex"
		authFilesProvider = "codex"
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	// 在提交回调前，先获取CPA中已有的邮箱列表（用于后续去重）
	existingEmails, err := s.cpaClient.GetExistingEmails(ctx, authFilesProvider)
	if err != nil {
		// 如果获取失败，记录日志但继续处理
		existingEmails = make(map[string]bool)
	}

	// 更新pending记录，保存已有邮箱列表和provider
	s.pendingMu.Lock()
	if p, ok := s.pendingAuths[req.State]; ok {
		p.ExistingEmails = existingEmails
		p.AuthFilesProvider = authFilesProvider
	}
	s.pendingMu.Unlock()

	// 提交回调给CPA
	callbackReq := &cpa.OAuthCallbackRequest{
		Provider:    oauthProvider,
		RedirectURL: req.CallbackURL,
		Code:        parsedURL.Code,
		State:       parsedURL.State,
		Error:       parsedURL.Error,
	}

	if err := s.cpaClient.SubmitOAuthCallback(ctx, callbackReq); err != nil {
		c.JSON(http.StatusInternalServerError, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "提交回调失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, AuthStatusResponse{
		Success: true,
		Status:  "submitted",
		Message: "回调已提交，正在处理中...",
	})
}

// ParsedCallbackURL 解析后的回调URL
type ParsedCallbackURL struct {
	State string
	Code  string
	Error string
}

// parseCallbackURL 解析回调URL
func parseCallbackURL(callbackURL string) (*ParsedCallbackURL, error) {
	// 尝试直接解析URL
	u, err := url.Parse(callbackURL)
	if err != nil {
		return nil, fmt.Errorf("无法解析URL")
	}

	q := u.Query()
	state := q.Get("state")
	code := q.Get("code")
	errMsg := q.Get("error")

	if state == "" {
		return nil, fmt.Errorf("缺少state参数")
	}

	if code == "" && errMsg == "" {
		return nil, fmt.Errorf("缺少code或error参数")
	}

	return &ParsedCallbackURL{
		State: state,
		Code:  code,
		Error: errMsg,
	}, nil
}

// AuthCompleteRequest 完成授权请求
type AuthCompleteRequest struct {
	State string `json:"state" binding:"required"`
}

func (s *Server) deletePendingAuth(state string) {
	s.pendingMu.Lock()
	delete(s.pendingAuths, state)
	s.pendingMu.Unlock()
}

// authCompleteHandler 确认授权完成并生成CDK
func (s *Server) authCompleteHandler(c *gin.Context) {
	var req AuthCompleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "请求参数错误",
		})
		return
	}

	// 检查是否是我们跟踪的授权
	s.pendingMu.RLock()
	pending, exists := s.pendingAuths[req.State]
	s.pendingMu.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "授权会话不存在或已过期",
		})
		return
	}

	// 使用保存的 authFilesProvider，如果为空则根据类型推断
	provider := pending.AuthFilesProvider
	if provider == "" {
		switch pending.Type {
		case "gemini_cli":
			provider = "gemini-cli"
		case "codex":
			provider = "codex"
		default:
			provider = "antigravity"
		}
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// 获取当前CPA中的凭证列表，找出新增的邮箱
	currentAuthFiles, err := s.cpaClient.GetAuthFiles(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "无法验证凭证状态: " + err.Error(),
		})
		return
	}

	// 找出新增的邮箱（在提交回调后新增的，且provider匹配的）
	var newEmail string
	for _, f := range currentAuthFiles.Files {
		if f.Email != "" && f.Provider == provider {
			// 检查这个邮箱是否在提交回调前就已存在
			// 如果 ExistingEmails 是 nil 或该邮箱不在其中，说明是新增的
			if pending.ExistingEmails == nil || !pending.ExistingEmails[f.Email] {
				newEmail = f.Email
				break
			}
		}
	}

	// 如果没有发现新邮箱，说明：
	// 1. 该邮箱之前已经存在（重复捐赠）
	// 2. 或者OAuth流程还未完成
	if newEmail == "" {
		// 检查是否有任何匹配provider的凭证存在
		hasAnyCredential := false
		for _, f := range currentAuthFiles.Files {
			if f.Email != "" && f.Provider == provider {
				hasAnyCredential = true
				break
			}
		}

		if hasAnyCredential && pending.ExistingEmails != nil && len(pending.ExistingEmails) > 0 {
			// 有凭证存在，且都在之前的列表中 - 说明是重复账号
			s.deletePendingAuth(req.State)
			c.JSON(http.StatusConflict, AuthStatusResponse{
				Success: false,
				Status:  "error",
				Message: "该账号已捐赠过，无法重复领取CDK",
			})
			return
		}

		// 可能OAuth还未完成
		c.JSON(http.StatusBadRequest, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "未检测到新凭证，请确认OAuth授权已完成",
		})
		return
	}

	// 使用邮箱作为唯一标识进行去重检查
	credHash := cpa.HashState(pending.Type, newEmail)

	// 检查是否已经领取过（防止重复领取）
	credExists, err := s.db.CheckCredentialExists(credHash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "服务器错误",
		})
		return
	}

	if credExists {
		s.deletePendingAuth(req.State)
		c.JSON(http.StatusConflict, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "该授权已领取过CDK",
		})
		return
	}

	// 从CDK池中获取一个可用的CDK（根据分组），如果CDK用完则自动创建新分组
	cdkRecord, err := s.getOrCreateCDK(pending.GroupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "获取CDK失败: " + err.Error(),
		})
		return
	}

	if cdkRecord == nil {
		c.JSON(http.StatusServiceUnavailable, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "CDK已发放完毕，请联系管理员补充",
		})
		return
	}

	// 创建凭证记录
	credential := &database.Credential{
		Type:           database.CredentialType(pending.Type),
		Email:          newEmail, // 使用从CPA获取的真实邮箱
		ProjectID:      "",
		CredentialHash: credHash,
		Status:         database.CredentialStatusVerified,
	}

	if err := s.db.CreateCredential(credential); err != nil {
		c.JSON(http.StatusInternalServerError, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "保存凭证失败",
		})
		return
	}

	// 将CDK分配给凭证
	if err := s.db.AssignCDKToCredential(cdkRecord.ID, credential.ID); err != nil {
		_, _ = s.db.DeleteCredential(credential.ID)
		c.JSON(http.StatusInternalServerError, AuthStatusResponse{
			Success: false,
			Status:  "error",
			Message: "分配CDK失败",
		})
		return
	}

	cdkCode := cdkRecord.Code

	// 更新凭证关联CDK
	_ = s.db.UpdateCredentialStatus(credential.ID, database.CredentialStatusVerified, &cdkRecord.ID)

	// 发送回调通知（可选）
	if s.cfg.CallbackURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		callbackData := &callback.CallbackData{
			CredentialID:   credential.ID,
			CredentialType: pending.Type,
			Email:          newEmail,
			ProjectID:      "",
			CDKCode:        cdkCode,
		}

		callbackResult, _ := s.notifier.Notify(ctx, callbackData)
		if callbackResult != nil {
			_ = s.db.SaveCallbackLog(
				credential.ID,
				s.cfg.CallbackURL,
				"",
				callbackResult.ResponseBody,
				callbackResult.StatusCode,
				callbackResult.Success,
			)
		}
	}

	s.deletePendingAuth(req.State)
	c.JSON(http.StatusOK, AuthStatusResponse{
		Success: true,
		Status:  "success",
		CDK:     cdkCode,
		Email:   newEmail,
		Message: "CDK已生成",
	})
}

// IFlowAuthRequest iFlow Cookie登录请求
type IFlowAuthRequest struct {
	Cookie  string `json:"cookie" binding:"required"`
	GroupID *int64 `json:"group_id"`
}

// iflowAuthHandler 处理iFlow Cookie登录
func (s *Server) iflowAuthHandler(c *gin.Context) {
	var req IFlowAuthRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, AuthStartResponse{
			Success: false,
			Message: "请求参数错误: " + err.Error(),
		})
		return
	}

	// 检查渠道是否启用
	channelEnabled, _ := s.db.GetSiteConfig("channel_iflow")
	if channelEnabled == "false" {
		c.JSON(http.StatusBadRequest, AuthStartResponse{
			Success: false,
			Message: "iFlow渠道已关闭",
		})
		return
	}

	// 验证Cookie格式
	if !strings.HasPrefix(req.Cookie, "BXAuth=") {
		c.JSON(http.StatusBadRequest, AuthStartResponse{
			Success: false,
			Message: "Cookie格式错误，应以BXAuth=开头",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	// 调用CPA提交iFlow Cookie
	iflowResp, err := s.cpaClient.SubmitIFlowCookie(ctx, req.Cookie)
	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "iFlow登录失败: " + err.Error(),
		})
		return
	}

	email := iflowResp.Email
	if email == "" {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "iFlow登录失败: 未获取到邮箱信息",
		})
		return
	}

	// 使用邮箱作为唯一标识进行去重检查
	credHash := cpa.HashState("iflow", email)

	// 检查是否已经领取过（防止重复领取）
	credExists, err := s.db.CheckCredentialExists(credHash)
	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "服务器错误",
		})
		return
	}

	if credExists {
		c.JSON(http.StatusConflict, AuthStartResponse{
			Success: false,
			Message: "该账号已捐赠过，无法重复领取CDK",
		})
		return
	}

	// 从CDK池中获取一个可用的CDK（根据分组），如果CDK用完则自动创建新分组
	cdkRecord, err := s.getOrCreateCDK(req.GroupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "获取CDK失败: " + err.Error(),
		})
		return
	}

	if cdkRecord == nil {
		c.JSON(http.StatusServiceUnavailable, AuthStartResponse{
			Success: false,
			Message: "CDK已发放完毕，请联系管理员补充",
		})
		return
	}

	// 创建凭证记录
	credential := &database.Credential{
		Type:           database.CredentialType("iflow"),
		Email:          email,
		ProjectID:      "",
		CredentialHash: credHash,
		Status:         database.CredentialStatusVerified,
	}

	if err := s.db.CreateCredential(credential); err != nil {
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "保存凭证失败",
		})
		return
	}

	// 将CDK分配给凭证
	if err := s.db.AssignCDKToCredential(cdkRecord.ID, credential.ID); err != nil {
		_, _ = s.db.DeleteCredential(credential.ID)
		c.JSON(http.StatusInternalServerError, AuthStartResponse{
			Success: false,
			Message: "分配CDK失败",
		})
		return
	}

	cdkCode := cdkRecord.Code

	// 更新凭证关联CDK
	_ = s.db.UpdateCredentialStatus(credential.ID, database.CredentialStatusVerified, &cdkRecord.ID)

	// 发送回调通知（可选）
	if s.cfg.CallbackURL != "" {
		callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer callbackCancel()

		callbackData := &callback.CallbackData{
			CredentialID:   credential.ID,
			CredentialType: "iflow",
			Email:          email,
			ProjectID:      "",
			CDKCode:        cdkCode,
		}

		callbackResult, _ := s.notifier.Notify(callbackCtx, callbackData)
		if callbackResult != nil {
			_ = s.db.SaveCallbackLog(
				credential.ID,
				s.cfg.CallbackURL,
				"",
				callbackResult.ResponseBody,
				callbackResult.StatusCode,
				callbackResult.Success,
			)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"cdk":     cdkCode,
		"email":   email,
		"message": "CDK已生成",
	})
}

// getSiteConfigHandler 获取站点配置
func (s *Server) getSiteConfigHandler(c *gin.Context) {
	bgImage, _ := s.db.GetSiteConfig("background_image")
	siteName, _ := s.db.GetSiteConfig("site_name")
	siteSubtitle, _ := s.db.GetSiteConfig("site_subtitle")

	if siteName == "" {
		siteName = s.cfg.SiteName
	}
	if bgImage == "" {
		bgImage = s.cfg.BackgroundImage
	}
	if siteSubtitle == "" {
		siteSubtitle = "感谢您的慷慨捐赠，让世界更美好"
	}

	c.JSON(http.StatusOK, gin.H{
		"site_name":        siteName,
		"background_image": bgImage,
		"site_subtitle":    siteSubtitle,
	})
}

// adminAuthMiddleware 管理员认证中间件
func (s *Server) adminAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		username, password, hasAuth := c.Request.BasicAuth()
		if !hasAuth || username != s.cfg.AdminUsername || password != s.cfg.AdminPassword {
			c.Header("WWW-Authenticate", `Basic realm="Admin Area"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

// statsHandler 获取统计数据
func (s *Server) statsHandler(c *gin.Context) {
	stats, err := s.db.GetStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stats)
}

// publicStatsHandler 公开统计数据（仅凭证数量，用于首页展示）
func (s *Server) publicStatsHandler(c *gin.Context) {
	stats, err := s.db.GetStats()
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"total_credentials": 0})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"total_credentials": stats["total_credentials"],
	})
}

func getPaginationParams(c *gin.Context) (int, int) {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if err != nil || offset < 0 {
		offset = 0
	}

	return limit, offset
}

// listCredentialsHandler 列出凭证
func (s *Server) listCredentialsHandler(c *gin.Context) {
	limit, offset := getPaginationParams(c)

	credentials, total, err := s.db.ListCredentials(limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   credentials,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// deleteCredentialHandler 删除凭证
func (s *Server) deleteCredentialHandler(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	deleted, err := s.db.DeleteCredential(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if deleted == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "凭证不存在"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "凭证删除成功"})
}

// listCDKsHandler 列出CDK
func (s *Server) listCDKsHandler(c *gin.Context) {
	limit, offset := getPaginationParams(c)

	cdks, total, err := s.db.ListCDKs(limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":   cdks,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// SetSiteConfigRequest 设置站点配置请求
type SetSiteConfigRequest struct {
	Key   string `json:"key" binding:"required"`
	Value string `json:"value" binding:"required"`
}

// setSiteConfigHandler 设置站点配置
func (s *Server) setSiteConfigHandler(c *gin.Context) {
	var req SetSiteConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 只允许设置特定的配置项
	allowedKeys := map[string]bool{
		"site_name":        true,
		"background_image": true,
		"site_subtitle":    true,
	}

	if !allowedKeys[req.Key] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid config key"})
		return
	}

	if err := s.db.SetSiteConfig(req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// AddCDKRequest 添加CDK请求
type AddCDKRequest struct {
	Code    string `json:"code" binding:"required"`
	GroupID *int64 `json:"group_id,omitempty"`
}

// addCDKHandler 添加单个CDK
func (s *Server) addCDKHandler(c *gin.Context) {
	var req AddCDKRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 检查是否已存在
	existing, err := s.db.GetCDKByCode(req.Code)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if existing != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "CDK已存在"})
		return
	}

	if err := s.db.AddCDK(req.Code, req.GroupID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "CDK添加成功"})
}

// BatchAddCDKRequest 批量添加CDK请求
type BatchAddCDKRequest struct {
	Codes   []string `json:"codes"` // JSON方式
	GroupID *int64   `json:"group_id,omitempty"`
}

// batchAddCDKHandler 批量导入CDK (支持JSON和txt文件)
func (s *Server) batchAddCDKHandler(c *gin.Context) {
	var codes []string
	var groupID *int64

	// 检查是否是文件上传
	file, err := c.FormFile("file")
	if err == nil {
		// 文件上传方式
		f, err := file.Open()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无法打开文件"})
			return
		}
		defer f.Close()

		// 读取文件内容
		content := make([]byte, file.Size)
		_, err = f.Read(content)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "无法读取文件"})
			return
		}

		// 按行分割，每行一个CDK
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			code := strings.TrimSpace(line)
			if code != "" {
				codes = append(codes, code)
			}
		}

		// 获取分组ID（从表单字段）
		if gidStr := c.PostForm("group_id"); gidStr != "" {
			if gid, err := strconv.ParseInt(gidStr, 10, 64); err == nil {
				groupID = &gid
			}
		}
	} else {
		// JSON方式
		var req BatchAddCDKRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请提供CDK列表或上传txt文件"})
			return
		}
		codes = req.Codes
		groupID = req.GroupID
	}

	if len(codes) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "没有有效的CDK"})
		return
	}

	added, skipped, err := s.db.BatchAddCDKs(codes, groupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"added":   added,
		"skipped": skipped,
		"total":   len(codes),
		"message": fmt.Sprintf("成功添加 %d 个CDK，跳过 %d 个重复", added, skipped),
	})
}

// deleteCDKHandler 删除CDK
func (s *Server) deleteCDKHandler(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	if err := s.db.DeleteCDK(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "CDK删除成功"})
}

// batchDeleteCDKHandler 批量删除CDK
func (s *Server) batchDeleteCDKHandler(c *gin.Context) {
	var req struct {
		IDs []int64 `json:"ids"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}

	if len(req.IDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请选择要删除的CDK"})
		return
	}

	deleted, err := s.db.BatchDeleteCDK(req.IDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "deleted": deleted})
}

// ====== 渠道管理 ======

// getChannelsHandler 获取渠道配置
func (s *Server) getChannelsHandler(c *gin.Context) {
	channels := map[string]bool{
		"antigravity": true, // 默认开启
		"gemini_cli":  true,
		"codex":       true,
		"iflow":       true,
	}

	for name := range channels {
		val, _ := s.db.GetSiteConfig("channel_" + name)
		if val == "false" {
			channels[name] = false
		}
	}

	c.JSON(http.StatusOK, channels)
}

// SetChannelRequest 设置渠道状态请求
type SetChannelRequest struct {
	Channel string `json:"channel" binding:"required"`
	Enabled bool   `json:"enabled"`
}

// setChannelHandler 设置渠道开关
func (s *Server) setChannelHandler(c *gin.Context) {
	var req SetChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 验证渠道名称
	validChannels := map[string]bool{"antigravity": true, "gemini_cli": true, "codex": true, "iflow": true}
	if !validChannels[req.Channel] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的渠道名称"})
		return
	}

	value := "true"
	if !req.Enabled {
		value = "false"
	}

	if err := s.db.SetSiteConfig("channel_"+req.Channel, value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ====== CDK分组管理 ======

// listCDKGroupsPublicHandler 公开的CDK分组列表（用于前端选择）
func (s *Server) listCDKGroupsPublicHandler(c *gin.Context) {
	groups, err := s.db.ListCDKGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, groups)
}

// listCDKGroupsHandler 列出CDK分组
func (s *Server) listCDKGroupsHandler(c *gin.Context) {
	groups, err := s.db.ListCDKGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, groups)
}

// CreateCDKGroupRequest 创建CDK分组请求
type CreateCDKGroupRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
}

// createCDKGroupHandler 创建CDK分组
func (s *Server) createCDKGroupHandler(c *gin.Context) {
	var req CreateCDKGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	group, err := s.db.CreateCDKGroup(req.Name, req.Description)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, group)
}

// updateCDKGroupHandler 更新CDK分组
func (s *Server) updateCDKGroupHandler(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	var req CreateCDKGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.db.UpdateCDKGroup(id, req.Name, req.Description); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// deleteCDKGroupHandler 删除CDK分组
func (s *Server) deleteCDKGroupHandler(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	// 检查是否强制删除（包括分组内所有CDK）
	force := c.Query("force") == "true"

	if force {
		// 强制删除：先删除分组内所有CDK，再删除分组
		if err := s.db.DeleteCDKGroupWithCDKs(id); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	} else {
		// 普通删除：仅删除空分组
		if err := s.db.DeleteCDKGroup(id); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "分组删除成功"})
}
