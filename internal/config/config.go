package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	CSVFilePath    string
	Port           string
	PostgresConfig PostgresConfig
	SMTPConfig     SMTPConfig
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
	To       string
}

func LoadConfig() (*Config, error) {
	csvFilePath := os.Getenv("CSV_FILE_PATH")
	if csvFilePath == "" {
		csvFilePath = "transactions.csv"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "4004"
	}

	pgHost := os.Getenv("POSTGRES_HOST")
	if pgHost == "" {
		return nil, fmt.Errorf("the POSTGRES_HOST environment variable is required")
	}

	pgPort, err := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	if err != nil {
		return nil, fmt.Errorf("the POSTGRES_PORT environment variable must be a valid integer")
	}

	pgUser := os.Getenv("POSTGRES_USER")
	if pgUser == "" {
		return nil, fmt.Errorf("the POSTGRES_USER environment variable is required")
	}

	pgPassword := os.Getenv("POSTGRES_PASSWORD")
	if pgPassword == "" {
		return nil, fmt.Errorf("the POSTGRES_PASSWORD environment variable is required")
	}

	pgDatabase := os.Getenv("POSTGRES_DATABASE")
	if pgDatabase == "" {
		return nil, fmt.Errorf("the POSTGRES_DATABASE environment variable is required")
	}

	postgresConfig := PostgresConfig{
		Host:     pgHost,
		Port:     pgPort,
		User:     pgUser,
		Password: pgPassword,
		Database: pgDatabase,
	}

	smtpHost := os.Getenv("SMTP_HOST")
	if smtpHost == "" {
		return nil, fmt.Errorf("the SMTP_HOST environment variable is required")
	}

	smtpPort, err := strconv.Atoi(os.Getenv("SMTP_PORT"))
	if err != nil {
		return nil, fmt.Errorf("the SMTP_PORT environment variable must be a valid integer")
	}

	smtpUsername := os.Getenv("SMTP_USERNAME")
	smtpPassword := os.Getenv("SMTP_PASSWORD")
	smtpFrom := os.Getenv("SMTP_FROM")
	if smtpFrom == "" {
		return nil, fmt.Errorf("the SMTP_FROM environment variable is required")
	}

	smtpTo := os.Getenv("SMTP_TO")
	if smtpTo == "" {
		return nil, fmt.Errorf("the SMTP_TO environment variable is required")
	}

	smtpConfig := SMTPConfig{
		Host:     smtpHost,
		Port:     smtpPort,
		Username: smtpUsername,
		Password: smtpPassword,
		From:     smtpFrom,
		To:       smtpTo,
	}

	config := &Config{
		CSVFilePath:    csvFilePath,
		Port:           port,
		PostgresConfig: postgresConfig,
		SMTPConfig:     smtpConfig,
	}

	return config, nil
}
