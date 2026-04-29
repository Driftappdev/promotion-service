package http

import (
	"log"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}

func AccessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)
		log.Printf("request_id=%s method=%s path=%s status=%d latency_ms=%d client_ip=%s",
			c.GetString("request_id"),
			c.Request.Method,
			c.FullPath(),
			c.Writer.Status(),
			latency.Milliseconds(),
			c.ClientIP(),
		)
	}
}

func RecoveryMiddleware() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		log.Printf("panic recovered request_id=%s err=%v stack=%s",
			c.GetString("request_id"), recovered, string(debug.Stack()))
		writeError(c, 500, "internal_error", "internal server error")
	})
}
