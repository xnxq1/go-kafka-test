package http_server

import (
	"context"
	"net/http"
)

func ErrorMapMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		errHolder := &ErrHolder{}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), "err", errHolder)))
		if errHolder.Error != nil {
			resp, statusCode := ErrorMapping(errHolder.Error)
			_ = WriteJson(resp, statusCode, w)
		}
	})
}
