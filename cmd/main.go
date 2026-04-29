package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"promotion-service/config"
	adapterhttp "promotion-service/internal/adapter/http"
	natsadmin "promotion-service/internal/adapter/nats_admin"
	repository "promotion-service/internal/adapter/repository"
	httpinfra "promotion-service/internal/integration/http"
	natsinfra "promotion-service/internal/integration/nats"
	"promotion-service/internal/integration/postgres"
	newshandler "promotion-service/internal/service_logic/handler/news"
	promotionhandler "promotion-service/internal/service_logic/handler/promotion"
	newsservice "promotion-service/internal/service_logic/service/news"
	promotionservice "promotion-service/internal/service_logic/service/promotion"

	"github.com/gin-gonic/gin"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lmicroseconds)

	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if cfg.App.Env == "prod" || cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	dsn := buildPostgresDSN(cfg)
	db, err := postgres.New(dsn)
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	defer db.Close()

	configureDBPool(db, cfg)
	if err := db.Ping(); err != nil {
		log.Fatalf("database ping failed: %v", err)
	}

	promotionRepo := repository.NewPostgresPromotionRepository(db)
	newsRepo := repository.NewPostgresNewsRepository(db)

	promotionSvc := promotionservice.NewPromotionService(promotionRepo)
	newsSvc := newsservice.NewNewsService(newsRepo)

	promotionFlow := promotionhandler.NewPromotionHandler(promotionSvc)
	newsFlow := newshandler.NewNewsHandler(newsSvc)
	healthFlow := adapterhttp.NewHealthHandler(postgres.NewHealthChecker(db))

	router := gin.New()
	router.Use(
		adapterhttp.RequestIDMiddleware(),
		adapterhttp.AccessLogMiddleware(),
		adapterhttp.RecoveryMiddleware(),
	)
	adapterhttp.RegisterRoutes(router, adapterhttp.Handlers{
		Promotion: adapterhttp.NewPromotionHandler(promotionFlow),
		News:      adapterhttp.NewNewsHandler(newsFlow),
		Health:    healthFlow,
	})

	httpServer := httpinfra.NewServer(
		fmt.Sprintf(":%d", cfg.Server.Port),
		router,
		time.Duration(cfg.Server.ReadTimeoutSec)*time.Second,
		time.Duration(cfg.Server.WriteTimeoutSec)*time.Second,
		time.Duration(cfg.Server.IdleTimeoutSec)*time.Second,
	)

	if cfg.NATS.Enabled {
		nc, err := natsinfra.NewConnection(natsinfra.Config{
			URL:           cfg.NATS.URL,
			MaxReconnect:  10,
			ReconnectWait: 2 * time.Second,
			ClientName:    "promotion-service",
		})
		if err != nil {
			log.Printf("nats connection failed: %v", err)
		} else {
			defer nc.Close()
			js, err := natsinfra.SetupJetStream(nc, natsinfra.StreamConfig{
				Name:     cfg.NATS.Stream,
				Subjects: []string{cfg.NATS.Subject},
				Replicas: 1,
			})
			if err != nil {
				log.Printf("nats stream setup failed: %v", err)
			} else {
				consumer := natsadmin.NewAdminPromotionConsumer(
					js,
					cfg.NATS.Stream,
					cfg.NATS.Subject,
					cfg.NATS.Durable,
					promotionSvc,
				)
				go func() {
					if err := consumer.Start(context.Background()); err != nil {
						log.Printf("admin promotion consumer stopped: %v", err)
					}
				}()
			}
		}
	}

	go func() {
		log.Printf("service=%s env=%s port=%d status=started", cfg.App.Name, cfg.App.Env, cfg.Server.Port)
		if err := httpServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("service=%s status=shutting_down", cfg.App.Name)
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(cfg.Server.ShutdownTimeoutSec)*time.Second,
	)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	log.Printf("service=%s status=stopped", cfg.App.Name)
}

func buildPostgresDSN(cfg *config.Config) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DB.Host,
		cfg.DB.Port,
		cfg.DB.User,
		cfg.DB.Password,
		cfg.DB.Name,
		cfg.DB.SSLMode,
	)
}

func configureDBPool(db *sql.DB, cfg *config.Config) {
	db.SetMaxOpenConns(cfg.DB.MaxOpenConns)
	db.SetMaxIdleConns(cfg.DB.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.DB.ConnMaxLifetimeMinute) * time.Minute)
}
