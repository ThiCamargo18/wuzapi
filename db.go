package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
)

type DatabaseConfig struct {
	Type     string
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	Path     string
	SSLMode  string
	Schema   string
}

// validSchemaName restricts DB_SCHEMA to plain identifiers so it can be safely
// interpolated into DDL (CREATE SCHEMA) and the connection string's search_path.
var validSchemaName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func InitializeDatabase(exPath, dataDirFlag string) (*sqlx.DB, error) {
	config := getDatabaseConfig(exPath, dataDirFlag)

	if config.Type == "postgres" {
		return initializePostgres(config)
	}
	return initializeSQLite(config)
}

func getDatabaseConfig(exPath, dataDirFlag string) DatabaseConfig {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbSSL := os.Getenv("DB_SSLMODE")
	dbSchema := strings.TrimSpace(os.Getenv("DB_SCHEMA"))

	sslMode := dbSSL
	if dbSSL == "true" {
		sslMode = "require"
	} else if dbSSL == "false" || dbSSL == "" {
		sslMode = "disable"
	}

	if dbUser != "" && dbPassword != "" && dbName != "" && dbHost != "" && dbPort != "" {
		return DatabaseConfig{
			Type:     "postgres",
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     dbName,
			SSLMode:  sslMode,
			Schema:   dbSchema,
		}
	}

	// Use datadir flag if provided, otherwise fall back to executable directory
	dataPath := exPath
	if dataDirFlag != "" {
		dataPath = dataDirFlag
	}

	return DatabaseConfig{
		Type: "sqlite",
		Path: filepath.Join(dataPath, "dbdata"),
	}
}

// postgresBaseDSN builds the connection string without a search_path, i.e.
// connecting to whatever schema is the server default (normally "public").
// It's used to bootstrap a custom schema before it exists.
func postgresBaseDSN(config DatabaseConfig) string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
		config.User, config.Password, config.Name, config.Host, config.Port, config.SSLMode,
	)
}

// postgresDSN builds the connection string used for the application pool. When
// a custom schema is configured, search_path is embedded in the DSN itself
// (rather than issued as a SET after connecting) so every pooled connection —
// including ones opened lazily later — gets it applied at the protocol level.
func postgresDSN(config DatabaseConfig) string {
	dsn := postgresBaseDSN(config)
	if config.Schema != "" {
		dsn = fmt.Sprintf("%s search_path=%s,public", dsn, config.Schema)
	}
	return dsn
}

// ensureSchemaExists creates config.Schema if it doesn't already exist, using
// a bootstrap connection on the server's default schema (so it works even
// before the target schema exists). Existence is checked first via pg_namespace
// (a plain catalog read, open to any role) and CREATE SCHEMA is only attempted
// when actually missing: Postgres enforces the CREATE-on-database privilege for
// that statement regardless of IF NOT EXISTS, so a role that was only granted
// USAGE/CREATE on its own pre-existing schema (and nothing at the database
// level, by design) would otherwise fail here even though there's nothing to do.
func ensureSchemaExists(config DatabaseConfig) error {
	if config.Schema == "" || config.Schema == "public" {
		return nil
	}

	bootstrap, err := sqlx.Open("postgres", postgresBaseDSN(config))
	if err != nil {
		return fmt.Errorf("failed to open postgres bootstrap connection: %w", err)
	}
	defer bootstrap.Close()

	if err := bootstrap.Ping(); err != nil {
		return fmt.Errorf("failed to ping postgres database: %w", err)
	}

	var exists bool
	if err := bootstrap.Get(&exists, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", config.Schema); err != nil {
		return fmt.Errorf("failed to check existence of schema %q: %w", config.Schema, err)
	}
	if exists {
		return nil
	}

	if _, err := bootstrap.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", config.Schema)); err != nil {
		return fmt.Errorf("failed to create schema %q: %w", config.Schema, err)
	}

	return nil
}

func initializePostgres(config DatabaseConfig) (*sqlx.DB, error) {
	if config.Schema != "" && !validSchemaName.MatchString(config.Schema) {
		return nil, fmt.Errorf("invalid DB_SCHEMA %q: must match ^[a-zA-Z_][a-zA-Z0-9_]*$", config.Schema)
	}

	if err := ensureSchemaExists(config); err != nil {
		return nil, err
	}

	dsn := postgresDSN(config)

	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	var databaseName, schemaName string
	if err := db.QueryRow("SELECT current_database(), current_schema()").Scan(&databaseName, &schemaName); err != nil {
		return nil, fmt.Errorf("failed to identify postgres database: %w", err)
	}
	log.Info().
		Str("driver", "postgres").
		Str("host", config.Host).
		Str("port", config.Port).
		Str("database", databaseName).
		Str("schema", schemaName).
		Str("user", config.User).
		Str("sslmode", config.SSLMode).
		Msg("Database connection established")

	return db, nil
}

func initializeSQLite(config DatabaseConfig) (*sqlx.DB, error) {
	if err := os.MkdirAll(config.Path, 0751); err != nil {
		return nil, fmt.Errorf("could not create dbdata directory: %w", err)
	}

	dbPath := filepath.ToSlash(filepath.Join(config.Path, "users.db"))
	db, err := sqlx.Open("sqlite", dbPath+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	return db, nil
}

type HistoryMessage struct {
	ID              int       `json:"id" db:"id"`
	UserID          string    `json:"user_id" db:"user_id"`
	ChatJID         string    `json:"chat_jid" db:"chat_jid"`
	SenderJID       string    `json:"sender_jid" db:"sender_jid"`
	MessageID       string    `json:"message_id" db:"message_id"`
	Timestamp       time.Time `json:"timestamp" db:"timestamp"`
	MessageType     string    `json:"message_type" db:"message_type"`
	TextContent     string    `json:"text_content" db:"text_content"`
	MediaLink       string    `json:"media_link" db:"media_link"`
	QuotedMessageID string    `json:"quoted_message_id,omitempty" db:"quoted_message_id"`
	DataJson        string    `json:"data_json" db:"datajson"`
}

func (s *server) saveMessageToHistory(userID, chatJID, senderJID, messageID, messageType, textContent, mediaLink, quotedMessageID, dataJson string) error {
	// Idempotent insert: HistorySync batches (and reconnects) routinely redeliver
	// messages already persisted via the live Message event. The (user_id, message_id)
	// unique constraint makes those duplicates an expected condition, not an error,
	// so skip them silently instead of failing the insert and logging at ERROR. See #292.
	// Rebind adapts the ? placeholders to the active driver ($1.. on Postgres,
	// ? on SQLite), so the query is defined once. ON CONFLICT DO NOTHING is valid
	// on both Postgres and modern SQLite.
	query := s.db.Rebind(`INSERT INTO message_history (user_id, chat_jid, sender_jid, message_id, timestamp, message_type, text_content, media_link, quoted_message_id, datajson)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT (user_id, message_id) DO NOTHING`)
	_, err := s.db.Exec(query, userID, chatJID, senderJID, messageID, time.Now(), messageType, textContent, mediaLink, quotedMessageID, dataJson)
	if err != nil {
		return fmt.Errorf("failed to save message to history: %w", err)
	}
	return nil
}

func (s *server) trimMessageHistory(userID, chatJID string, limit int) error {
	var queryHistory, querySecrets string

	if s.db.DriverName() == "postgres" {
		queryHistory = `
            DELETE FROM message_history
            WHERE id IN (
                SELECT id FROM message_history
                WHERE user_id = $1 AND chat_jid = $2
                ORDER BY timestamp DESC
                OFFSET $3
            )`

		querySecrets = `
            DELETE FROM whatsmeow_message_secrets
            WHERE message_id IN (
                SELECT message_id FROM message_history
                WHERE user_id = $1 AND chat_jid = $2
                ORDER BY timestamp DESC
                OFFSET $3
            )`
	} else { // sqlite
		queryHistory = `
            DELETE FROM message_history
            WHERE id IN (
                SELECT id FROM message_history
                WHERE user_id = ? AND chat_jid = ?
                ORDER BY timestamp DESC
                LIMIT -1 OFFSET ?
            )`

		querySecrets = `
            DELETE FROM whatsmeow_message_secrets
            WHERE message_id IN (
                SELECT message_id FROM message_history
                WHERE user_id = ? AND chat_jid = ?
                ORDER BY timestamp DESC
                LIMIT -1 OFFSET ?
            )`
	}

	if _, err := s.db.Exec(querySecrets, userID, chatJID, limit); err != nil {
		return fmt.Errorf("failed to trim message secrets: %w", err)
	}

	if _, err := s.db.Exec(queryHistory, userID, chatJID, limit); err != nil {
		return fmt.Errorf("failed to trim message history: %w", err)
	}

	return nil
}
