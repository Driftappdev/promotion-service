package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type ErrorResponse struct {
	RequestID string `json:"request_id,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

func writeError(c *gin.Context, status int, code, msg string) {
	requestID := c.GetString("request_id")
	c.AbortWithStatusJSON(status, ErrorResponse{
		RequestID: requestID,
		Code:      code,
		Message:   msg,
	})
}

func writeOK(c *gin.Context, payload any) {
	c.JSON(http.StatusOK, payload)
}
