package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"unicode"
)

type apiConfig struct {
	listen             string
	dsn                string
	clickhouseURL      string
	clickhouseDatabase string
	clickhouseUser     string
	clickhousePassword string
}

func loadAPIConfig(getenv func(string) string) (apiConfig, error) {
	if getenv("CERBERO_SECURITY_PROFILE") != "DEVELOPMENT" {
		return apiConfig{}, fmt.Errorf("Cerbero API is development-only until authentication/RBAC is wired")
	}
	listen := getenv("CERBERO_API_LISTEN_ADDRESS")
	postgresHost := getenv("POSTGRES_HOST")
	postgresPort := getenv("POSTGRES_PORT")
	postgresDatabase := getenv("POSTGRES_DB")
	postgresUser := getenv("POSTGRES_API_USER")
	postgresPassword := getenv("POSTGRES_API_PASSWORD")
	clickhouseHost := getenv("CLICKHOUSE_HOST")
	clickhousePort := getenv("CLICKHOUSE_HTTP_PORT")
	clickhouseDatabase := getenv("CLICKHOUSE_DB")
	clickhouseUser := getenv("CLICKHOUSE_API_USER")
	clickhousePassword := getenv("CLICKHOUSE_API_PASSWORD")
	for name, value := range map[string]string{
		"CERBERO_API_LISTEN_ADDRESS": listen,
		"POSTGRES_HOST":              postgresHost,
		"POSTGRES_PORT":              postgresPort,
		"POSTGRES_DB":                postgresDatabase,
		"POSTGRES_API_USER":          postgresUser,
		"POSTGRES_API_PASSWORD":      postgresPassword,
		"CLICKHOUSE_HOST":            clickhouseHost,
		"CLICKHOUSE_HTTP_PORT":       clickhousePort,
		"CLICKHOUSE_DB":              clickhouseDatabase,
		"CLICKHOUSE_API_USER":        clickhouseUser,
		"CLICKHOUSE_API_PASSWORD":    clickhousePassword,
	} {
		if strings.TrimSpace(value) == "" {
			return apiConfig{}, fmt.Errorf("%s is required", name)
		}
	}
	if !simpleConfigIdentifier(clickhouseDatabase) {
		return apiConfig{}, fmt.Errorf("CLICKHOUSE_DB must be a simple ClickHouse identifier")
	}
	dsn := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(postgresUser, postgresPassword),
		Host:   postgresHost + ":" + postgresPort,
		Path:   postgresDatabase,
	}
	return apiConfig{
		listen:             listen,
		dsn:                dsn.String(),
		clickhouseURL:      "http://" + clickhouseHost + ":" + clickhousePort,
		clickhouseDatabase: clickhouseDatabase,
		clickhouseUser:     clickhouseUser,
		clickhousePassword: clickhousePassword,
	}, nil
}

func simpleConfigIdentifier(value string) bool {
	for index, character := range value {
		if index == 0 {
			if character != '_' && !unicode.IsLetter(character) {
				return false
			}
			continue
		}
		if character != '_' && !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			return false
		}
	}
	return value != ""
}

func envConfig() (apiConfig, error) { return loadAPIConfig(os.Getenv) }
