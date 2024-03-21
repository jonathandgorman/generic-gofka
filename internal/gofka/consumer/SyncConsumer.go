package consumer

import (
	"context"
	"github.com/IBM/sarama"
	"log"
)

type SyncConsumer struct {
	sarama.ConsumerGroup
}

type ConsumerHandler struct {
	Channel chan string
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Received message: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		h.Channel <- string(msg.Value)
		session.MarkMessage(msg, "")
	}
	return nil
}

func InitializeSyncConsumer(brokers []string, topics []string, groupID string) (*sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	handler := &ConsumerHandler{}
	go func() {
		for _, topic := range topics {
			err := consumer.Consume(context.Background(), []string{topic}, handler)
			if err != nil {
				log.Printf("Error consuming topic %s: %v\n", topic, err)
				return
			}
		}
	}()

	return &consumer, nil
}
