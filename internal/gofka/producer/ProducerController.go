package producer

import (
	"io"
	"log"
	"net/http"
)

type KafkaProducerController struct {
	Service *KafkaProducerService
}

func (s *KafkaProducerService) ProduceHandler(writer http.ResponseWriter, request *http.Request) {
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Println("Error closing body", err)
		}
	}(request.Body)

	err := request.ParseForm()
	if err != nil {
		log.Println("Error parsing request", err)
		return
	}

	key := request.FormValue("key")
	value := request.FormValue("value")

	err = s.Produce(key, value)
	if err != nil {
		http.Error(writer, "Error producing message", http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
}
