package http

import "github.com/gin-gonic/gin"
import "github.com/diftappdev/libpackage/adminshield"

type Handlers struct {
	Promotion *PromotionHandler
	News      *NewsHandler
	Health    *HealthHandler
}

func RegisterRoutes(router *gin.Engine, h Handlers) {
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
