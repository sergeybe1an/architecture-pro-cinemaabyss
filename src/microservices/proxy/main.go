package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type ProxyConfig struct {
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

func main() {
	config := loadConfig()

	monolithProxy := newReverseProxy(config.MonolithURL)
	moviesProxy := newReverseProxy(config.MoviesServiceURL)
	eventsProxy := newReverseProxy(config.EventsServiceURL)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		if shouldRouteToMoviesService(config) {
			moviesProxy.ServeHTTP(w, r)
			return
		}
		monolithProxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/movies/", func(w http.ResponseWriter, r *http.Request) {
		if shouldRouteToMoviesService(config) {
			moviesProxy.ServeHTTP(w, r)
			return
		}
		monolithProxy.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/users", monolithProxy.ServeHTTP)
	mux.HandleFunc("/api/users/", monolithProxy.ServeHTTP)
	mux.HandleFunc("/api/payments", monolithProxy.ServeHTTP)
	mux.HandleFunc("/api/payments/", monolithProxy.ServeHTTP)
	mux.HandleFunc("/api/subscriptions", monolithProxy.ServeHTTP)
	mux.HandleFunc("/api/subscriptions/", monolithProxy.ServeHTTP)
	mux.HandleFunc("/api/events/", eventsProxy.ServeHTTP)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	log.Printf("Starting proxy service on port %s (gradual_migration=%v, movies_migration_percent=%d)",
		port, config.GradualMigration, config.MoviesMigrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func loadConfig() ProxyConfig {
	gradualMigration := parseBool(os.Getenv("GRADUAL_MIGRATION"), true)
	percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil {
		percent = 0
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	monolithURL := os.Getenv("MONOLITH_URL")
	if monolithURL == "" {
		monolithURL = "http://localhost:8080"
	}
	moviesURL := os.Getenv("MOVIES_SERVICE_URL")
	if moviesURL == "" {
		moviesURL = "http://localhost:8081"
	}
	eventsURL := os.Getenv("EVENTS_SERVICE_URL")
	if eventsURL == "" {
		eventsURL = "http://localhost:8082"
	}

	return ProxyConfig{
		MonolithURL:            monolithURL,
		MoviesServiceURL:       moviesURL,
		EventsServiceURL:       eventsURL,
		GradualMigration:       gradualMigration,
		MoviesMigrationPercent: percent,
	}
}

func parseBool(value string, defaultValue bool) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		return defaultValue
	}
}

func shouldRouteToMoviesService(config ProxyConfig) bool {
	if !config.GradualMigration {
		return true
	}

	if config.MoviesMigrationPercent >= 100 {
		return true
	}
	if config.MoviesMigrationPercent <= 0 {
		return false
	}

	return rand.Intn(100) < config.MoviesMigrationPercent
}

func newReverseProxy(targetURL string) *httputil.ReverseProxy {
	target, err := url.Parse(targetURL)
	if err != nil {
		log.Fatalf("Invalid target URL %q: %v", targetURL, err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
	}

	return proxy
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
}
