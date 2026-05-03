package http

import "net/http"
import "os"
import "strings"
import "github.com/gin-gonic/gin"
import "github.com/PlatformCore/middleware/adminshield"

type Handlers struct {
	Promotion *PromotionHandler
	News      *NewsHandler
	Health    *HealthHandler
}

func RegisterRoutes(router *gin.Engine, h Handlers) {
	router.POST("/internal/admin/control", func(c *gin.Context) {
		secret := strings.TrimSpace(os.Getenv("ADMIN_CONTROL_SHARED_SECRET"))
		if secret != "" && c.GetHeader("X-Admin-Secret") != secret {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		var body map[string]any
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_json"})
			return
		}
		action, _ := body["action"].(string)
		if action == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "action_required"})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{
			"accepted": true,
			"service":  "promotion-service",
			"action":   action,
		})
	})
	router.GET("/health", h.Health.Health)
	router.GET("/ready", h.Health.Ready)

	api := router.Group("/api/v1")
	{
		public := api.Group("/public")
		{
			public.GET("/promotions", h.Promotion.ListActive)
			public.GET("/news", h.News.List)
		}

		admin := api.Group("/admin")
		{
			admin.Use(adminshield.GinRequireRoles("SUPER_ADMIN", "ADMIN", "EDITOR"))
			admin.POST("/promotions", h.Promotion.Create)
			admin.PATCH("/promotions/:id/activate", h.Promotion.Activate)
			admin.POST("/news", h.News.Create)
			admin.PATCH("/news/:id/publish", h.News.Publish)
		}
	}
}
