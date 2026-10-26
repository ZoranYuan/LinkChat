package middleware

import (
	"log"

	"github.com/gin-gonic/gin"
)

func ErrorLoggerMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Next()

		status := ctx.Writer.Status()

		if len(ctx.Errors) > 0 || status >= 500 {
			errText := ctx.Errors.String()
			if errText == "" {
				if err := ctx.Err(); err != nil {
					errText = err.Error()
				}
			}
			if errText == "" {
				errText = "无错误详情"
			}
			log.Printf("[ERROR] %d | %s %s | %v",
				status,
				ctx.Request.Method,
				ctx.Request.URL.Path,
				errText,
			)
			return
		}

		if status >= 400 {
			log.Printf("[WARN] %d | %s %s",
				status,
				ctx.Request.Method,
				ctx.Request.URL.Path,
			)
		}
	}
}
