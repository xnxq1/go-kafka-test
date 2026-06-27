package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/go-chi/chi/v5"
	http_server "github.com/xnxq1/go-kafka-test/internal/http-server/messages"
	"github.com/xnxq1/go-kafka-test/internal/infra/postgres"
	logic "github.com/xnxq1/go-kafka-test/internal/logic/messages"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dbPool, err := postgres.NewPool(ctx, "")
	if err != nil {
		fmt.Println("Incorrect shutdown")
		os.Exit(1)
	}
	transactor := postgres.NewTransactor(dbPool)
	messageRepo := postgres.NewMessageRepo(dbPool)
	outboxMessageRepo := postgres.NewMessageOutboxRepo(dbPool)
	messageService := logic.NewMessageService(transactor, messageRepo, outboxMessageRepo)
	messageHandler := http_server.NewMessageHandler(messageService)
	messageRouter := messageHandler.Init()
	router := chi.NewRouter()
	router.Mount("/", messageRouter)
	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	go func() {
		fmt.Println("Listening on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("Incorrect shutdown")
		}
	}()
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt)
	<-exit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		fmt.Println("Incorrect shutdown")
	}
}
