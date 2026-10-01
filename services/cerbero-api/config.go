package main

import (
	"fmt"
	"net/url"
	"os"
)

type apiConfig struct {
	listen string
	dsn    string
}

func loadAPIConfig(getenv func(string) string) (apiConfig, error) {
	if getenv("CERBERO_SECURITY_PROFILE") != "DEVELOPMENT" {
		return apiConfig{}, fmt.Errorf("Step 30 API is development-only until RBAC is wired")
	}
	listen, host, port, database := getenv("CERBERO_API_LISTEN_ADDRESS"), getenv("POSTGRES_HOST"), getenv("POSTGRES_PORT"), getenv("POSTGRES_DB")
	user, password := getenv("POSTGRES_API_USER"), getenv("POSTGRES_API_PASSWORD")
	if listen == "" || host == "" || port == "" || database == "" || user == "" || password == "" {
		return apiConfig{}, fmt.Errorf("incomplete cerbero-api configuration")
	}
	dsn := &url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: host + ":" + port, Path: database}
	return apiConfig{listen: listen, dsn: dsn.String()}, nil
}
func envConfig() (apiConfig, error) { return loadAPIConfig(os.Getenv) }
