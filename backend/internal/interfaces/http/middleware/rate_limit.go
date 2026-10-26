package middleware

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	ratelimitport "IM_backend/internal/application/ports/ratelimit"
	"IM_backend/internal/interfaces/http/response"
)

type RateLimitMiddleware struct {
	limiter  ratelimitport.Limiter
	failOpen bool
}

func NewRateLimitMiddleware(limiter ratelimitport.Limiter, failOpen bool) *RateLimitMiddleware {
	return &RateLimitMiddleware{
		limiter:  limiter,
		failOpen: failOpen,
	}
}

func (m *RateLimitMiddleware) ByIP(scope string, policy ratelimitport.Policy) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ip := ctx.ClientIP()

		key := fmt.Sprintf(
			"http:%s:ip:%s",
			scope,
			ip,
		)

		decision, err := m.limiter.Allow(
			ctx.Request.Context(),
			key,
			policy,
		)

		if err != nil {
			// 限流器不可用
			log.Printf(
				"限流器执行失败，key=%s：%v",
				key,
				err,
			)

			if m.failOpen {
				ctx.Next()
				return
			}

			// 降级处理
			ctx.AbortWithStatusJSON(http.StatusServiceUnavailable,
				response.Error(
					http.StatusServiceUnavailable,
					"服务暂时不可用",
				))

			return
		}

		if decision.Allowed {
			ctx.Next()
			return
		}

		retrySeconds := int64(math.Ceil(decision.RetryAfter.Seconds()))

		ctx.Header(
			"Retry-After",
			strconv.FormatInt(
				retrySeconds,
				10,
			),
		)

		ctx.AbortWithStatusJSON(http.StatusTooManyRequests,
			response.Error(
				http.StatusTooManyRequests,
				"请求过于频繁，请稍后重试",
			))
	}
}
