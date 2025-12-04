package main

import (
	"context"
	"flag"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/rawnly/fedelytics/internal/routes"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/voxelite-ai/env"
)

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	log.Info().Msg("Logger initialized")
}

func main() {
	conn, err := connect()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}

	r := gin.Default()

	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	{
		// PROJECTS
		r.
			POST("/projects", routes.CreateProject(conn)).
			GET("/projects", routes.GetProjects(conn))
	}

	{
		// TELEMETRY
		r.
			POST("/projects/:p_id/ingest", routes.IngestEvents(conn))
	}

	r.Run()
}

func connect() (driver.Conn, error) {
	connectionURL := env.String("CLICKHOUSE_URL", "localhost:9000", "ClickHouse server URL")

	database := env.String("CLICKHOUSE_DB", "default", "ClickHouse database name")
	username := env.String("CLICKHOUSE_USER", "admin", "ClickHouse username")
	password := env.String("CLICKHOUSE_PASSWORD", "admin", "ClickHouse password")

	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{connectionURL},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{
					Name:    "fedelytics",
					Version: "1.0.0",
				},
			},
		},
		Debugf: func(format string, v ...interface{}) {
			log.Debug().Msgf(format, v...)
		},
	})
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if ex, ok := err.(*clickhouse.Exception); ok {
			log.Error().Msgf("Catch exception [%d] %s \n%s\n", ex.Code, ex.Message, ex.StackTrace)
		}

		return nil, err
	}

	return conn, nil
}
