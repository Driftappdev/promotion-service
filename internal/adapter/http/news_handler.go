package http

import (
	"database/sql"
	"net/http"
	"strconv"

	httpport "promotion-service/internal/interface/http"

	"github.com/gin-gonic/gin"
)

type NewsHandler struct {
	service httpport.NewsHTTPPort
}

func NewNewsHandler(service httpport.NewsHTTPPort) *NewsHandler {
	return &NewsHandler{service: service}
}

func (h *NewsHandler) List(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	items, err := h.service.List(c.Request.Context(), limit, offset)
	if err != nil {
		writeError(c, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}

	writeOK(c, gin.H{"data": items, "count": len(items)})
}

func (h *NewsHandler) Create(c *gin.Context) {
	var req httpport.CreateNewsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	created, err := h.service.Create(c.Request.Context(), req)
	if err != nil {
		writeError(c, http.StatusBadRequest, "create_failed", err.Error())
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (h *NewsHandler) Publish(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "id is required")
		return
	}
	if err := h.service.Publish(c.Request.Context(), id); err != nil {
		if err == sql.ErrNoRows {
			writeError(c, http.StatusNotFound, "not_found", "news not found")
			return
		}
		writeError(c, http.StatusInternalServerError, "publish_failed", err.Error())
		return
	}
	writeOK(c, gin.H{"status": "published"})
}
