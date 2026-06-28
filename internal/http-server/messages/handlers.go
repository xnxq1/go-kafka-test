package http_server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/xnxq1/go-kafka-test/internal/domain"
	http_server "github.com/xnxq1/go-kafka-test/internal/http-server"
)

type MessageHandler struct {
	messageService IMessageService
	config         IConfig
}

var OffsetInvalidError = errors.New("offset must be an integer")
var LimitInvalidError = errors.New("limit must be an integer")

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
	_ = http_server.WriteJson(
		http_server.SuccessResponse[*domain.Message]{Data: msg},
		200,
		w)
}

// GetMessages возвращает список сообщений с пагинацией.
// @Summary      Список сообщений
// @Description  Возвращает сообщения с пагинацией через query-параметры offset/limit
// @Tags         messages
// @Produce      json
// @Param        offset query int false "Смещение"
// @Param        limit  query int false "Лимит"
// @Success      200 {object} http_server.PaginationResponse[[]domain.Message]
// @Failure      400 {object} http_server.ErrorResponse
// @Failure      500 {object} http_server.ErrorResponse
// @Router       /messages [get]
func (handler *MessageHandler) GetMessages(w http.ResponseWriter, r *http.Request) {
	offset, limit := 0, handler.config.GetLimit()
	offsetStr := http_server.GetQueryParam(r, "offset")
	limitStr := http_server.GetQueryParam(r, "limit")
	var err error
	if offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			http_server.SetupError(r, OffsetInvalidError)
			return
		}
	}
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			http_server.SetupError(r, LimitInvalidError)
			return
		}
	}
	messages, err := handler.messageService.GetMessages(r.Context(), limit, offset)
	if err != nil {
		http_server.SetupError(r, err)
		return
	}
	_ = http_server.WriteJson(
		http_server.PaginationResponse[[]domain.Message]{
			SuccessResponse: http_server.SuccessResponse[[]domain.Message]{Data: messages},
			Pagination:      http_server.Pagination{Limit: limit, Offset: offset},
		}, 200, w)

}
func (handler *MessageHandler) Init() *chi.Mux {
	router := chi.NewRouter()
	router.Route("/messages", func(r chi.Router) {
		r.Post("/", handler.CreateMessage)
		r.Get("/", handler.GetMessages)
	})
	return router
}

func NewMessageHandler(messageService IMessageService, config IConfig) *MessageHandler {
	return &MessageHandler{messageService, config}
}
