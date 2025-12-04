package routes

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type Project struct {
	ID          string    `json:"project_id"`
	Name        string    `json:"name"`
	Owner       string    `json:"owner"`
	CreatedAt   time.Time `json:"created_at"`
	Description string    `json:"description"`
}

type CreateProjectPayload struct {
	Name        string `json:"name" binding:"required"`
	Owner       string `json:"owner" binding:"required"`
	Description string `json:"description"`
}

func CreateProject(conn driver.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		var payload CreateProjectPayload
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		err := conn.Exec(ctx,
			"INSERT INTO projects.list (name, owner, description) VALUES (?, ?, ?)",
			payload.Name, payload.Owner, payload.Description,
		)
		if err != nil {
			log.Error().Err(err).Msg("Failed to insert project")
			c.JSON(500, gin.H{"error": "Internal server error"})
			return
		}

		c.JSON(201, map[string]any{})
	}
}

func GetProjects(conn driver.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		rows, err := conn.Query(ctx, "SELECT project_id, name, owner, created_at, description FROM projects.list")
		if err != nil {
			log.Error().Err(err).Msg("Failed to query projects")
			c.JSON(500, gin.H{"error": "Internal server error"})
			return
		}
		defer rows.Close()

		var projects []Project
		for rows.Next() {
			var p Project
			if err := rows.Scan(&p.ID, &p.Name, &p.Owner, &p.CreatedAt, &p.Description); err != nil {
				log.Error().Err(err).Msg("Failed to scan project row")
				c.JSON(500, gin.H{"error": "Internal server error"})
				return
			}
			projects = append(projects, p)
		}

		c.JSON(200, projects)
	}
}
