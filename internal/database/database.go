package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// DB 数据库包装器
type DB struct {
	*sql.DB
}

// CredentialType 凭证类型
type CredentialType string

const (
	CredentialTypeAntigravity CredentialType = "antigravity"
	CredentialTypeGeminiCLI   CredentialType = "gemini_cli"
	CredentialTypeCodex       CredentialType = "codex"
)

// CredentialStatus 凭证状态
type CredentialStatus string

const (
	CredentialStatusPending  CredentialStatus = "pending"
	CredentialStatusVerified CredentialStatus = "verified"
	CredentialStatusInvalid  CredentialStatus = "invalid"
	CredentialStatusUsed     CredentialStatus = "used"
)

// Credential 凭证模型
type Credential struct {
	ID             int64            `json:"id"`
	Type           CredentialType   `json:"type"`
	Email          string           `json:"email"`
	ProjectID      string           `json:"project_id"`
	CredentialHash string           `json:"-"` // 凭证哈希，不暴露
	Status         CredentialStatus `json:"status"`
	CDKID          *int64           `json:"cdk_id,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

// CDK CDK模型
type CDK struct {
	ID           int64      `json:"id"`
	Code         string     `json:"code"`
	GroupID      *int64     `json:"group_id,omitempty"`
	GroupName    string     `json:"group_name,omitempty"`
	CredentialID *int64     `json:"credential_id,omitempty"`
	IsUsed       bool       `json:"is_used"`
	CreatedAt    time.Time  `json:"created_at"`
	UsedAt       *time.Time `json:"used_at,omitempty"`
}

// CDKGroup CDK分组模型
type CDKGroup struct {
	ID             int64     `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	CDKCount       int       `json:"cdk_count,omitempty"`
	AvailableCount int       `json:"available_count,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// Init 初始化数据库连接
func Init(dbURL string) (*DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	wrappedDB := &DB{db}

	// 初始化表
	if err := wrappedDB.initTables(); err != nil {
		return nil, fmt.Errorf("failed to init tables: %w", err)
	}

	return wrappedDB, nil
}

// initTables 创建数据库表
func (db *DB) initTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS credentials (
			id SERIAL PRIMARY KEY,
			type VARCHAR(50) NOT NULL,
			email VARCHAR(255) NOT NULL,
			project_id VARCHAR(255) NOT NULL,
			credential_hash VARCHAR(255) NOT NULL UNIQUE,
			status VARCHAR(50) NOT NULL DEFAULT 'pending',
			cdk_id INTEGER,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_credentials_hash ON credentials(credential_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_credentials_email ON credentials(email)`,
		`CREATE INDEX IF NOT EXISTS idx_credentials_status ON credentials(status)`,
		`CREATE TABLE IF NOT EXISTS cdks (
			id SERIAL PRIMARY KEY,
			code VARCHAR(100) NOT NULL UNIQUE,
			credential_id INTEGER REFERENCES credentials(id),
			is_used BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			used_at TIMESTAMP WITH TIME ZONE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_cdks_code ON cdks(code)`,
		`CREATE INDEX IF NOT EXISTS idx_cdks_used ON cdks(is_used)`,
		`CREATE TABLE IF NOT EXISTS callback_logs (
			id SERIAL PRIMARY KEY,
			credential_id INTEGER REFERENCES credentials(id),
			callback_url VARCHAR(500) NOT NULL,
			request_body TEXT,
			response_body TEXT,
			status_code INTEGER,
			success BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS site_config (
			id SERIAL PRIMARY KEY,
			key VARCHAR(100) NOT NULL UNIQUE,
			value TEXT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS cdk_groups (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='cdks' AND column_name='group_id') THEN
				ALTER TABLE cdks ADD COLUMN group_id INTEGER REFERENCES cdk_groups(id);
			END IF;
		END $$`,
		`CREATE INDEX IF NOT EXISTS idx_cdks_group ON cdks(group_id)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return nil
}

// CheckCredentialExists 检查凭证是否已存在
func (db *DB) CheckCredentialExists(credentialHash string) (bool, error) {
	var exists bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM credentials WHERE credential_hash = $1)`, credentialHash).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check credential: %w", err)
	}
	return exists, nil
}

// CreateCredential 创建凭证记录
func (db *DB) CreateCredential(cred *Credential) error {
	query := `
		INSERT INTO credentials (type, email, project_id, credential_hash, status)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at, updated_at
	`
	err := db.QueryRow(query, cred.Type, cred.Email, cred.ProjectID, cred.CredentialHash, cred.Status).
		Scan(&cred.ID, &cred.CreatedAt, &cred.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create credential: %w", err)
	}
	return nil
}

// UpdateCredentialStatus 更新凭证状态
func (db *DB) UpdateCredentialStatus(id int64, status CredentialStatus, cdkID *int64) error {
	query := `
		UPDATE credentials 
		SET status = $1, cdk_id = $2, updated_at = NOW()
		WHERE id = $3
	`
	_, err := db.Exec(query, status, cdkID, id)
	if err != nil {
		return fmt.Errorf("failed to update credential: %w", err)
	}
	return nil
}

// GetCredentialByID 根据ID获取凭证
func (db *DB) GetCredentialByID(id int64) (*Credential, error) {
	cred := &Credential{}
	query := `SELECT id, type, email, project_id, credential_hash, status, cdk_id, created_at, updated_at 
			  FROM credentials WHERE id = $1`
	err := db.QueryRow(query, id).Scan(
		&cred.ID, &cred.Type, &cred.Email, &cred.ProjectID, &cred.CredentialHash,
		&cred.Status, &cred.CDKID, &cred.CreatedAt, &cred.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get credential: %w", err)
	}
	return cred, nil
}

// GetCredentialByHash 根据哈希获取凭证
func (db *DB) GetCredentialByHash(hash string) (*Credential, error) {
	cred := &Credential{}
	query := `SELECT id, type, email, project_id, credential_hash, status, cdk_id, created_at, updated_at 
			  FROM credentials WHERE credential_hash = $1`
	err := db.QueryRow(query, hash).Scan(
		&cred.ID, &cred.Type, &cred.Email, &cred.ProjectID, &cred.CredentialHash,
		&cred.Status, &cred.CDKID, &cred.CreatedAt, &cred.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get credential: %w", err)
	}
	return cred, nil
}

// ListCredentials 获取凭证列表
func (db *DB) ListCredentials(limit, offset int) ([]*Credential, int, error) {
	var total int
	err := db.QueryRow(`SELECT COUNT(*) FROM credentials`).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count credentials: %w", err)
	}

	query := `
		SELECT id, type, email, project_id, credential_hash, status, cdk_id, created_at, updated_at 
		FROM credentials 
		ORDER BY created_at DESC 
		LIMIT $1 OFFSET $2
	`
	rows, err := db.Query(query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list credentials: %w", err)
	}
	defer rows.Close()

	var credentials []*Credential
	for rows.Next() {
		cred := &Credential{}
		err := rows.Scan(
			&cred.ID, &cred.Type, &cred.Email, &cred.ProjectID, &cred.CredentialHash,
			&cred.Status, &cred.CDKID, &cred.CreatedAt, &cred.UpdatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan credential: %w", err)
		}
		credentials = append(credentials, cred)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate credentials: %w", err)
	}

	return credentials, total, nil
}

// DeleteCredential 删除凭证，并清理本地关联关系
func (db *DB) DeleteCredential(id int64) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var exists bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM credentials WHERE id = $1)`, id).Scan(&exists); err != nil {
		return 0, fmt.Errorf("failed to check credential: %w", err)
	}
	if !exists {
		return 0, nil
	}

	// CDK 可能已经展示给用户，删除凭证时只解除关联，不自动回收到可用池。
	if _, err := tx.Exec(`UPDATE cdks SET credential_id = NULL WHERE credential_id = $1`, id); err != nil {
		return 0, fmt.Errorf("failed to unlink CDK: %w", err)
	}

	if _, err := tx.Exec(`DELETE FROM callback_logs WHERE credential_id = $1`, id); err != nil {
		return 0, fmt.Errorf("failed to delete callback logs: %w", err)
	}

	result, err := tx.Exec(`DELETE FROM credentials WHERE id = $1`, id)
	if err != nil {
		return 0, fmt.Errorf("failed to delete credential: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get affected rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return affected, nil
}

// CreateCDK 创建CDK
func (db *DB) CreateCDK(cdk *CDK) error {
	query := `
		INSERT INTO cdks (code, credential_id, is_used)
		VALUES ($1, $2, $3)
		RETURNING id, created_at
	`
	err := db.QueryRow(query, cdk.Code, cdk.CredentialID, cdk.IsUsed).Scan(&cdk.ID, &cdk.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to create CDK: %w", err)
	}
	return nil
}

// GetCDKByCode 根据code获取CDK
func (db *DB) GetCDKByCode(code string) (*CDK, error) {
	cdk := &CDK{}
	query := `SELECT id, code, credential_id, is_used, created_at, used_at FROM cdks WHERE code = $1`
	err := db.QueryRow(query, code).Scan(&cdk.ID, &cdk.Code, &cdk.CredentialID, &cdk.IsUsed, &cdk.CreatedAt, &cdk.UsedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get CDK: %w", err)
	}
	return cdk, nil
}

// GetCDKByCredentialID 根据凭证ID获取CDK
func (db *DB) GetCDKByCredentialID(credentialID int64) (*CDK, error) {
	cdk := &CDK{}
	query := `SELECT id, code, credential_id, is_used, created_at, used_at FROM cdks WHERE credential_id = $1`
	err := db.QueryRow(query, credentialID).Scan(&cdk.ID, &cdk.Code, &cdk.CredentialID, &cdk.IsUsed, &cdk.CreatedAt, &cdk.UsedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get CDK: %w", err)
	}
	return cdk, nil
}

// ListCDKs 获取CDK列表
func (db *DB) ListCDKs(limit, offset int) ([]*CDK, int, error) {
	var total int
	err := db.QueryRow(`SELECT COUNT(*) FROM cdks`).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count cdks: %w", err)
	}

	query := `
		SELECT c.id, c.code, c.group_id, c.credential_id, c.is_used, c.created_at, c.used_at,
			   COALESCE(g.name, '') as group_name
		FROM cdks c
		LEFT JOIN cdk_groups g ON c.group_id = g.id
		ORDER BY c.created_at DESC 
		LIMIT $1 OFFSET $2
	`
	rows, err := db.Query(query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list cdks: %w", err)
	}
	defer rows.Close()

	var cdks []*CDK
	for rows.Next() {
		cdk := &CDK{}
		err := rows.Scan(&cdk.ID, &cdk.Code, &cdk.GroupID, &cdk.CredentialID, &cdk.IsUsed, &cdk.CreatedAt, &cdk.UsedAt, &cdk.GroupName)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan cdk: %w", err)
		}
		cdks = append(cdks, cdk)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate cdks: %w", err)
	}

	return cdks, total, nil
}

// SaveCallbackLog 保存回调日志
func (db *DB) SaveCallbackLog(credentialID int64, callbackURL, requestBody, responseBody string, statusCode int, success bool) error {
	query := `
		INSERT INTO callback_logs (credential_id, callback_url, request_body, response_body, status_code, success)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := db.Exec(query, credentialID, callbackURL, requestBody, responseBody, statusCode, success)
	if err != nil {
		return fmt.Errorf("failed to save callback log: %w", err)
	}
	return nil
}

// GetSiteConfig 获取站点配置
func (db *DB) GetSiteConfig(key string) (string, error) {
	var value string
	err := db.QueryRow(`SELECT value FROM site_config WHERE key = $1`, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get site config: %w", err)
	}
	return value, nil
}

// SetSiteConfig 设置站点配置
func (db *DB) SetSiteConfig(key, value string) error {
	query := `
		INSERT INTO site_config (key, value, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()
	`
	_, err := db.Exec(query, key, value)
	if err != nil {
		return fmt.Errorf("failed to set site config: %w", err)
	}
	return nil
}

// GetStats 获取统计数据
func (db *DB) GetStats() (map[string]int, error) {
	stats := make(map[string]int)

	queries := map[string]string{
		"total_credentials":    "SELECT COUNT(*) FROM credentials",
		"pending_credentials":  "SELECT COUNT(*) FROM credentials WHERE status = 'pending'",
		"verified_credentials": "SELECT COUNT(*) FROM credentials WHERE status = 'verified'",
		"invalid_credentials":  "SELECT COUNT(*) FROM credentials WHERE status = 'invalid'",
		"total_cdks":           "SELECT COUNT(*) FROM cdks",
		"used_cdks":            "SELECT COUNT(*) FROM cdks WHERE is_used = true",
		"available_cdks":       "SELECT COUNT(*) FROM cdks WHERE is_used = false AND credential_id IS NULL",
	}

	for key, query := range queries {
		var count int
		if err := db.QueryRow(query).Scan(&count); err != nil {
			return nil, fmt.Errorf("failed to get stat %s: %w", key, err)
		}
		stats[key] = count
	}

	return stats, nil
}

// GetAvailableCDK 获取一个可用的CDK（未使用且未分配的，支持按分组筛选）
func (db *DB) GetAvailableCDK(groupID *int64) (*CDK, error) {
	cdk := &CDK{}
	var query string
	var err error

	if groupID != nil {
		query = `SELECT id, code, group_id, credential_id, is_used, created_at, used_at 
				  FROM cdks 
				  WHERE is_used = false AND credential_id IS NULL AND group_id = $1
				  ORDER BY created_at ASC 
				  LIMIT 1`
		err = db.QueryRow(query, *groupID).Scan(&cdk.ID, &cdk.Code, &cdk.GroupID, &cdk.CredentialID, &cdk.IsUsed, &cdk.CreatedAt, &cdk.UsedAt)
	} else {
		query = `SELECT id, code, group_id, credential_id, is_used, created_at, used_at 
				  FROM cdks 
				  WHERE is_used = false AND credential_id IS NULL 
				  ORDER BY created_at ASC 
				  LIMIT 1`
		err = db.QueryRow(query).Scan(&cdk.ID, &cdk.Code, &cdk.GroupID, &cdk.CredentialID, &cdk.IsUsed, &cdk.CreatedAt, &cdk.UsedAt)
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get available CDK: %w", err)
	}
	return cdk, nil
}

// AssignCDKToCredential 将CDK分配给凭证
func (db *DB) AssignCDKToCredential(cdkID, credentialID int64) error {
	now := time.Now()
	query := `
		UPDATE cdks
		SET credential_id = $1, is_used = true, used_at = $2
		WHERE id = $3 AND is_used = false AND credential_id IS NULL
	`
	result, err := db.Exec(query, credentialID, now, cdkID)
	if err != nil {
		return fmt.Errorf("failed to assign CDK: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("CDK不可用或已被分配")
	}
	return nil
}

// AddCDK 添加单个CDK到池中（支持分组）
func (db *DB) AddCDK(code string, groupID *int64) error {
	query := `INSERT INTO cdks (code, group_id, is_used) VALUES ($1, $2, false) ON CONFLICT (code) DO NOTHING`
	_, err := db.Exec(query, code, groupID)
	if err != nil {
		return fmt.Errorf("failed to add CDK: %w", err)
	}
	return nil
}

// BatchAddCDKs 批量添加CDK（支持分组）
func (db *DB) BatchAddCDKs(codes []string, groupID *int64) (int, int, error) {
	added := 0
	skipped := 0

	for _, code := range codes {
		// 检查是否已存在
		var exists bool
		err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM cdks WHERE code = $1)`, code).Scan(&exists)
		if err != nil {
			return added, skipped, fmt.Errorf("failed to check CDK: %w", err)
		}

		if exists {
			skipped++
			continue
		}

		// 添加CDK
		_, err = db.Exec(`INSERT INTO cdks (code, group_id, is_used) VALUES ($1, $2, false)`, code, groupID)
		if err != nil {
			return added, skipped, fmt.Errorf("failed to add CDK: %w", err)
		}
		added++
	}

	return added, skipped, nil
}

// DeleteCDK 删除CDK
func (db *DB) DeleteCDK(id int64) error {
	_, err := db.Exec(`DELETE FROM cdks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete CDK: %w", err)
	}
	return nil
}

// BatchDeleteCDK 批量删除CDK
func (db *DB) BatchDeleteCDK(ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	// 构建 IN 子句的占位符
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`DELETE FROM cdks WHERE id IN (%s)`, strings.Join(placeholders, ","))
	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to batch delete CDKs: %w", err)
	}

	affected, _ := result.RowsAffected()
	return affected, nil
}

// ====== CDK分组管理 ======

// CreateCDKGroup 创建CDK分组
func (db *DB) CreateCDKGroup(name, description string) (*CDKGroup, error) {
	group := &CDKGroup{}
	query := `INSERT INTO cdk_groups (name, description) VALUES ($1, $2) RETURNING id, name, description, created_at`
	err := db.QueryRow(query, name, description).Scan(&group.ID, &group.Name, &group.Description, &group.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create CDK group: %w", err)
	}
	return group, nil
}

// ListCDKGroups 列出所有CDK分组
func (db *DB) ListCDKGroups() ([]*CDKGroup, error) {
	query := `SELECT g.id, g.name, g.description, g.created_at,
				COALESCE((SELECT COUNT(*) FROM cdks WHERE group_id = g.id), 0) as cdk_count,
				COALESCE((SELECT COUNT(*) FROM cdks WHERE group_id = g.id AND is_used = false AND credential_id IS NULL), 0) as available_count
			  FROM cdk_groups g
			  ORDER BY g.created_at DESC`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list CDK groups: %w", err)
	}
	defer rows.Close()

	var groups []*CDKGroup
	for rows.Next() {
		group := &CDKGroup{}
		err := rows.Scan(&group.ID, &group.Name, &group.Description, &group.CreatedAt, &group.CDKCount, &group.AvailableCount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan CDK group: %w", err)
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate CDK groups: %w", err)
	}
	return groups, nil
}

// GetCDKGroup 获取单个CDK分组
func (db *DB) GetCDKGroup(id int64) (*CDKGroup, error) {
	group := &CDKGroup{}
	query := `SELECT g.id, g.name, g.description, g.created_at,
				COALESCE((SELECT COUNT(*) FROM cdks WHERE group_id = g.id), 0) as cdk_count,
				COALESCE((SELECT COUNT(*) FROM cdks WHERE group_id = g.id AND is_used = false AND credential_id IS NULL), 0) as available_count
			  FROM cdk_groups g WHERE g.id = $1`
	err := db.QueryRow(query, id).Scan(&group.ID, &group.Name, &group.Description, &group.CreatedAt, &group.CDKCount, &group.AvailableCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get CDK group: %w", err)
	}
	return group, nil
}

// UpdateCDKGroup 更新CDK分组
func (db *DB) UpdateCDKGroup(id int64, name, description string) error {
	query := `UPDATE cdk_groups SET name = $1, description = $2 WHERE id = $3`
	_, err := db.Exec(query, name, description, id)
	if err != nil {
		return fmt.Errorf("failed to update CDK group: %w", err)
	}
	return nil
}

// DeleteCDKGroup 删除CDK分组（仅当分组内没有CDK时可删除）
func (db *DB) DeleteCDKGroup(id int64) error {
	// 检查是否有CDK
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM cdks WHERE group_id = $1`, id).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check CDKs in group: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("该分组下还有 %d 个CDK，请先移除", count)
	}

	_, err = db.Exec(`DELETE FROM cdk_groups WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete CDK group: %w", err)
	}
	return nil
}

// DeleteCDKGroupWithCDKs 强制删除CDK分组及其所有CDK
func (db *DB) DeleteCDKGroupWithCDKs(id int64) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 先删除分组内所有CDK
	_, err = tx.Exec(`DELETE FROM cdks WHERE group_id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete CDKs in group: %w", err)
	}

	// 再删除分组
	_, err = tx.Exec(`DELETE FROM cdk_groups WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete CDK group: %w", err)
	}

	return tx.Commit()
}
