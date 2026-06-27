package http_server

import (
	"encoding/json"
	"errors"
	"net/http"
)

type SuccessResponse[T any] struct {
	Data T `json:"data"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type Pagination struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}
type PaginationResponse[T any] struct {
	SuccessResponse[T]
	Pagination Pagination `json:"pagination"`
}

var DecodeError = errors.New("error decoding data")

var EncodeError = errors.New("error encoding data")

func DecodeJson(r *http.Request, v any) error {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(v); err != nil {
		return err
	}
	return nil
}

func WriteJson(response any, status_code int, w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status_code)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		return err
	}
	return nil
}
func GetQueryParam(r *http.Request, key string) string {
	return r.URL.Query().Get(key)
}
