// Command server runs the durable-streams HTTP server.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jilio/durable-streams-with-ralph/server"
	"github.com/jilio/durable-streams-with-ralph/stream"
)

func main() {
	port := flag.Int("port", 8080, "HTTP server port")
	timeout := flag.Duration("timeout", 30*time.Second, "Long-poll timeout")
	flag.Parse()

	// Create storage and server
	storage := stream.NewMemoryStorage()
	srv := server.NewWithOptions(storage, "", *timeout)

	// Setup HTTP server
	addr := fmt.Sprintf(":%d", *port)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Server is shutting down...")
		httpServer.Close()
		close(done)
	}()

	log.Printf("Durable Streams server starting on %s\n", addr)
	log.Printf("Long-poll timeout: %v\n", *timeout)

	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}

	<-done
	log.Println("Server stopped")
}
