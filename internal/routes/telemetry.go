package routes

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func IngestEvents(conn driver.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		var payload IngestTelemetryPayload
		if err := c.ShouldBindJSON(&payload); err != nil {
			log.Error().Err(err).Msg("Failed to bind JSON payload")
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO telemetry.events")
		if err != nil {
			log.Error().Err(err).Msg("Failed to prepare batch")
			c.JSON(500, gin.H{"error": "Internal server error"})
			return
		}
		defer batch.Close()

		for _, event := range payload.Events {
			event.ProjectID = c.Param("p_id")
			event.EventTime = time.Now().UTC()
			event.IngestedAt = time.Now().UTC()
			event.SessionID = uuid.NewString()

			log.Debug().Interface("event", event).Msg("Ingesting telemetry event")

			if err := batch.AppendStruct(&event); err != nil {
				log.Error().Err(err).Msg("Failed to append event to batch")
				c.JSON(500, gin.H{"error": "Internal server error"})
				return
			}
		}

		if err := batch.Send(); err != nil {
			log.Error().Err(err).Msg("Failed to send batch")
			c.JSON(500, gin.H{"error": "Internal server error"})
			return
		}

		c.JSON(200, gin.H{
			"ingested_events": len(payload.Events),
		})
	}
}

type TelemetryEvent struct {
	EventTime  time.Time      `ch:"event_time"`
	IngestedAt time.Time      `ch:"ingested_at"`
	ProjectID  string         `ch:"project_id"`
	UserID     string         `ch:"user_id" json:"user_id" binding:"required"`
	SessionID  string         `ch:"session_id"`
	CliVersion string         `ch:"cli_version" json:"cli_version" binding:"required"`
	Command    string         `ch:"command" json:"command" binding:"required"`
	ExitCode   int32          `ch:"exit_code" json:"exit_code" binding:"required"`
	OS         string         `ch:"os" json:"os" binding:"required"`
	Arch       string         `ch:"arch" json:"arch" binding:"required"`
	LatencyMs  float32        `ch:"latency_ms" json:"latency_ms" binding:"required"`
	Success    uint8          `ch:"success" json:"success" binding:"required"`
	Extra      map[string]any `ch:"extra" json:"extra"`
}

type IngestTelemetryPayload struct {
	Events []TelemetryEvent `json:"events" binding:"required"`
}
