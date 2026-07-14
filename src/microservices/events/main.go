package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicMovieEvents   = "movie-events"
	topicUserEvents    = "user-events"
	topicPaymentEvents = "payment-events"
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type MovieEventInput struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEventInput struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEventInput struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type,omitempty"`
}

type EventService struct {
	brokers []string
	writers map[string]*kafka.Writer
}

func main() {
	brokers := getBrokers()
	service := newEventService(brokers)
	defer service.close()

	for _, topic := range []string{topicMovieEvents, topicUserEvents, topicPaymentEvents} {
		go consumeTopic(brokers, topic)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", handleHealth)
	mux.HandleFunc("/api/events/movie", service.handleMovieEvent)
	mux.HandleFunc("/api/events/user", service.handleUserEvent)
	mux.HandleFunc("/api/events/payment", service.handlePaymentEvent)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("Starting events microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func getBrokers() []string {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		return []string{"localhost:9092"}
	}

	parts := strings.Split(brokersEnv, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func newEventService(brokers []string) *EventService {
	writers := map[string]*kafka.Writer{
		topicMovieEvents:   newWriter(brokers, topicMovieEvents),
		topicUserEvents:    newWriter(brokers, topicUserEvents),
		topicPaymentEvents: newWriter(brokers, topicPaymentEvents),
	}

	return &EventService{
		brokers: brokers,
		writers: writers,
	}
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
}

func (s *EventService) close() {
	for _, writer := range s.writers {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close Kafka writer: %v", err)
		}
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func (s *EventService) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var input MovieEventInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if input.MovieID == 0 || input.Title == "" || input.Action == "" {
		writeError(w, "movie_id, title and action are required", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s-%d", input.MovieID, input.Action, time.Now().UnixNano()),
		Type:      "movie",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   input,
	}

	s.publishEvent(w, topicMovieEvents, event)
}

func (s *EventService) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var input UserEventInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if input.UserID == 0 || input.Action == "" || input.Timestamp == "" {
		writeError(w, "user_id, action and timestamp are required", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("user-%d-%s-%d", input.UserID, input.Action, time.Now().UnixNano()),
		Type:      "user",
		Timestamp: input.Timestamp,
		Payload:   input,
	}

	s.publishEvent(w, topicUserEvents, event)
}

func (s *EventService) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var input PaymentEventInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if input.PaymentID == 0 || input.UserID == 0 || input.Status == "" || input.Timestamp == "" {
		writeError(w, "payment_id, user_id, status and timestamp are required", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s-%d", input.PaymentID, input.Status, time.Now().UnixNano()),
		Type:      "payment",
		Timestamp: input.Timestamp,
		Payload:   input,
	}

	s.publishEvent(w, topicPaymentEvents, event)
}

func (s *EventService) publishEvent(w http.ResponseWriter, topic string, event Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	writer := s.writers[topic]
	message := kafka.Message{
		Key:   []byte(event.ID),
		Value: payload,
	}
	if err := writer.WriteMessages(ctx, message); err != nil {
		log.Printf("Failed to publish event to topic %s: %v", topic, err)
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		Status:    "success",
		Partition: message.Partition,
		Offset:    message.Offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(response)
}

func consumeTopic(brokers []string, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "events-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Failed to close Kafka reader for topic %s: %v", topic, err)
		}
	}()

	log.Printf("Started Kafka consumer for topic %s", topic)

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka consumer error on topic %s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("Consumed event from topic %s [partition=%d offset=%d]: %s",
			topic, message.Partition, message.Offset, string(message.Value))
	}
}

func writeError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
