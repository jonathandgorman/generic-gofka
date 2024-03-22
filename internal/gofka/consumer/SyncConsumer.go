package consumer

import (
	"generic-gofka/internal/gofka/model"
	"github.com/IBM/sarama"
)

type Handler struct {
	Messages chan<- *model.KafkaMessage
}

func (h Handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h Handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		kafkaMessage := model.KafkaMessage{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       string(msg.Key),
			Value:     string(msg.Value),
		}

		h.Messages <- &kafkaMessage
		session.MarkMessage(msg, "")
	}
	return nil
}

func InitializeSyncConsumer(brokers []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	return client, nil
}
