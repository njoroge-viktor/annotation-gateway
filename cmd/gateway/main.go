// Command gateway serves the annotation task API.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/njoroge-viktor/annotation-gateway/internal/api"
	"github.com/njoroge-viktor/annotation-gateway/internal/metrics"
	"github.com/njoroge-viktor/annotation-gateway/internal/queue"
	"github.com/njoroge-viktor/annotation-gateway/internal/ratelimit"
)

func main() {
	var (
		addr            = flag.String("addr", ":8080", "listen address")
		leaseTTL        = flag.Duration("lease-ttl", 5*time.Minute, "how long a claimed task stays leased")
		maxAttempts     = flag.Int("max-attempts", 3, "re-lease attempts before a task is dropped (0 = unlimited)")
		burst           = flag.Int("rate-burst", 20, "rate limit burst per annotator")
		perSecond       = flag.Float64("rate-per-second", 5, "rate limit refill per second per annotator")
		shutdownTimeout = flag.Duration("shutdown-timeout", 15*time.Second, "grace period for in-flight requests")
		logLevel        = flag.String("log-level", "info", "debug, info, warn or error")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(logger)

	if err := run(*addr, *leaseTTL, *maxAttempts, *burst, *perSecond, *shutdownTimeout, logger); err != nil {
		logger.Error("server terminated", "error", err)
		os.Exit(1)
	}
}

func run(
	addr string,
	leaseTTL time.Duration,
	maxAttempts, burst int,
	perSecond float64,
	shutdownTimeout time.Duration,
	logger *slog.Logger,
) error {
	q := queue.New(leaseTTL, queue.WithMaxAttempts(maxAttempts))
	limiter := ratelimit.New(burst, perSecond)
	registry := metrics.NewRegistry()

	stopJanitor := make(chan struct{})
	defer close(stopJanitor)
	limiter.StartJanitor(time.Minute, stopJanitor)

	server := &http.Server{
		Addr:              addr,
		Handler:           api.New(q, limiter, registry, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second, // slowloris
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Signal handling before ListenAndServe, so a signal arriving during
	// startup is not lost.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", addr, "lease_ttl", leaseTTL.String())
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining", "timeout", shutdownTimeout.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		// Drain exceeded the grace period: close hard rather than hang.
		_ = server.Close()
		return err
	}

	logger.Info("shutdown complete")
	return nil
}

func parseLevel(name string) slog.Level {
	switch name {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
