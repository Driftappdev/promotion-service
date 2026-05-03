package servicecore

import "net/http"

// HTTPMiddleware defines standard servicecore HTTP middleware signature.
type HTTPMiddleware func(http.Handler) http.Handler

// DefaultHTTPMiddlewares returns enterprise baseline middleware chain placeholders.
func DefaultHTTPMiddlewares() []HTTPMiddleware {
	return []HTTPMiddleware{}
}
