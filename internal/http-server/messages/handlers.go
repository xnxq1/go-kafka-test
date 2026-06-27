package http_server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	http_server "github.com/xnxq1/go-kafka-test/internal/http-server"
)

type MessageHandler struct {
}

func (handler *MessageHandler) CreateMessage(w http.ResponseWriter, r *http.Request) {
	var req CreateMessageRequest
	if err := http_server.DecodeJson(r, &req); err != nil {
		http_server.SetupError(r, err)
		return
	}
}
func (handler *MessageHandler) Init() *chi.Mux {
	router := chi.NewRouter()
	router.Route("/messages", func(r chi.Router) {
		r.Post("/", handler.CreateMessage)
	})
	return router
}
