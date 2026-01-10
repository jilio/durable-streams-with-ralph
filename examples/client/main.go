// Command client demonstrates the durable-streams client library.
// It can produce messages to a stream or consume messages from it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jilio/durable-streams-with-ralph/client"
	"github.com/jilio/durable-streams-with-ralph/stream"
)

func main() {
	// Command flags
	streamURL := flag.String("url", "http://localhost:8080/demo/stream", "Stream URL")
	mode := flag.String("mode", "consume", "Mode: produce, consume, or both")
	count := flag.Int("count", 10, "Number of messages to produce")
	interval := flag.Duration("interval", 100*time.Millisecond, "Interval between produced messages")
	offset := flag.String("offset", "-1", "Starting offset for consumption (-1 for start, 'now' for tail)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("Shutting down...")
		cancel()
	}()

	c := client.New(*streamURL, client.WithContentType("application/json"))

	// Ensure stream exists
	if err := ensureStream(ctx, c); err != nil {
		log.Fatalf("Failed to ensure stream: %v", err)
	}

	switch *mode {
	case "produce":
		if err := produce(ctx, c, *count, *interval); err != nil {
			log.Fatalf("Producer error: %v", err)
		}
	case "consume":
		if err := consume(ctx, c, stream.Offset(*offset)); err != nil {
			log.Fatalf("Consumer error: %v", err)
		}
	case "both":
		// Run producer and consumer concurrently
		done := make(chan error, 2)
		go func() {
			done <- produce(ctx, c, *count, *interval)
		}()
		go func() {
			done <- consume(ctx, c, stream.Offset(*offset))
		}()
		if err := <-done; err != nil {
			log.Printf("Error: %v", err)
		}
	default:
		log.Fatalf("Unknown mode: %s (use: produce, consume, or both)", *mode)
	}
}

// ensureStream creates the stream if it doesn't exist.
func ensureStream(ctx context.Context, c *client.Client) error {
	result, err := c.Head(ctx)
	if err != nil {
		return err
	}

	if result.Exists {
		log.Printf("Stream exists, offset: %s", result.Offset)
		return nil
	}

	log.Println("Creating stream...")
	err = c.Create(ctx, client.CreateOptions{
		ContentType: "application/json",
	})
	if err != nil && err != client.ErrStreamExists {
		return err
	}

	log.Println("Stream created")
	return nil
}

// produce sends messages to the stream.
func produce(ctx context.Context, c *client.Client, count int, interval time.Duration) error {
	log.Printf("Producing %d messages with %v interval...", count, interval)

	for i := 1; i <= count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg := map[string]interface{}{
			"index":     i,
			"timestamp": time.Now().Format(time.RFC3339Nano),
			"message":   fmt.Sprintf("Hello from producer #%d", i),
		}

		offset, err := c.AppendJSON(ctx, msg)
		if err != nil {
			return fmt.Errorf("failed to append message %d: %w", i, err)
		}

		log.Printf("Produced message %d, next offset: %s", i, offset)

		if i < count {
			time.Sleep(interval)
		}
	}

	log.Println("Producer finished")
	return nil
}

// consume subscribes to the stream and prints received messages.
func consume(ctx context.Context, c *client.Client, offset stream.Offset) error {
	log.Printf("Subscribing from offset: %s", offset)

	sub := c.Subscribe(ctx, client.SubscribeOptions{
		Offset: offset,
	})

	msgCount := 0
	for result := range sub.Messages {
		for _, msg := range result.Messages {
			msgCount++

			// Pretty print JSON messages
			var data interface{}
			if err := json.Unmarshal(msg.Data, &data); err == nil {
				prettyJSON, _ := json.MarshalIndent(data, "", "  ")
				log.Printf("Received message #%d:\n%s", msgCount, string(prettyJSON))
			} else {
				log.Printf("Received message #%d: %s", msgCount, string(msg.Data))
			}
		}
		log.Printf("Current offset: %s", sub.Offset())
	}

	if err := sub.Err(); err != nil && err != context.Canceled {
		return fmt.Errorf("subscription error: %w", err)
	}

	log.Printf("Consumer finished, received %d messages", msgCount)
	return nil
}
