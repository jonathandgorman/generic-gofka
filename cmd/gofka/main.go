package main

import (
	"generic-gofka/internal/gofka/consumer"
	"generic-gofka/internal/gofka/producer"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	brokers := []string{"127.0.0.1:9092"}
	topics := []string{"gofka-topic"}
	groupID := "gofka-group"

	// setup sync kafka producer service
	syncProducer, err := producer.NewSyncProducer(brokers)
	if err != nil {
		log.Println("Something went wrong when setting up the producer", err)
		os.Exit(1)
	}
	producerService := producer.KafkaProducerService{SyncProducer: syncProducer}
	producerController := producer.KafkaProducerController{Service: &producerService}

	_, err = consumer.InitializeSyncConsumer(brokers, topics, groupID)
	if err != nil {
		log.Fatal("Error initializing Kafka consumer: ", err)
	}

	// create router
	router := mux.NewRouter()
	router.HandleFunc("/produce", producerController.Service.ProduceHandler)

	// close producers and consumers elegantly
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-signalChannel
		log.Printf("Received signal %v. Shutting down...\n", sig)

		err := syncProducer.Close()
		if err != nil {
			log.Println("Error closing Kafka syncProducer", err)
		}

		os.Exit(0)
	}()

	log.Println("Server listening on port 9000...")
	err = http.ListenAndServe(":9000", router)
	if err != nil {
		log.Fatal("Failed to handle request: ", err)
	}
}
