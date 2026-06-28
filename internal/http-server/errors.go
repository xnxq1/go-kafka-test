package http_server

import (
	"errors"
	"net/http"
)

type ErrHolder struct {
	Error error
}

type ErrKey struct{}

func ErrorMapping(err error) (ErrorResponse, int) {
	statusCode := http.StatusInternalServerError
	switch {
	case errors.Is(err, DecodeError), errors.Is(err, EncodeError):
		statusCode = http.StatusBadRequest
	}
	return ErrorResponse{Error: err.Error()}, statusCode

}

func SetupError(r *http.Request, err error) {
	holder := r.Context().Value(ErrKey{}).(*ErrHolder)
	holder.Error = err
}
