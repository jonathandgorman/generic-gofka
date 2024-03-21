package producer

import (
	"github.com/IBM/sarama"
	"log"
)

const (
	topic = "gofka-topic"
)

type KafkaProducerService struct {
	SyncProducer *SyncProducer
}

func (s *KafkaProducerService) Produce(key string, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	_, _, err := s.SyncProducer.SendMessage(msg)
	if err != nil {
		log.Println("Something went wrong when sending the message", err)
		return err
	}
	return nil
}
