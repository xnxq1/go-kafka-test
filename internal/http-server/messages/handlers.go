package http_server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/xnxq1/go-kafka-test/internal/domain"
	http_server "github.com/xnxq1/go-kafka-test/internal/http-server"
)

type MessageHandler struct {
	messageService IMessageService
}

// CreateMessage создаёт новое сообщение.
// @Summary      Создать сообщение
// @Description  Создаёт сообщение и кладёт событие в outbox в одной транзакции
// @Tags         messages
// @Accept       json
// @Produce      json
// @Param        request body CreateMessageRequest true "Тело запроса"
// @Success      200 {object} domain.Message
// @Failure      400 {object} http_server.ErrorResponse
// @Failure      500 {object} http_server.ErrorResponse
// @Router       /messages [post]
func (handler *MessageHandler) CreateMessage(w http.ResponseWriter, r *http.Request) {
	var req CreateMessageRequest
	if err := http_server.DecodeJson(r, &req); err != nil {
		http_server.SetupError(r, err)
		return
	}
	var msg *domain.Message
	msg, err := handler.messageService.CreateMessage(r.Context(), req.Content)
	if err != nil {
		http_server.SetupError(r, err)
		return
	}
	_ = http_server.WriteJson(msg, 200, w)
}
func (handler *MessageHandler) Init() *chi.Mux {
	router := chi.NewRouter()
	router.Route("/messages", func(r chi.Router) {
		r.Post("/", handler.CreateMessage)
	})
	return router
}

func NewMessageHandler(messageService IMessageService) *MessageHandler {
	return &MessageHandler{messageService}
}
