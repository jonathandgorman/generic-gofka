package producer

import (
	"github.com/IBM/sarama"
)

type SyncProducer struct {
	SaramaSyncProducer *sarama.SyncProducer
}

func NewSyncProducer(brokers []string) (*SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true // required to be set for SaramaSyncProducer

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	syncProducer := SyncProducer{&producer}
	return &syncProducer, nil
}

func (p *SyncProducer) SendMessage(message *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	partition, offset, err = (*p.SaramaSyncProducer).SendMessage(message)
	return partition, offset, err
}

func (p *SyncProducer) Close() error {
	err := (*p.SaramaSyncProducer).Close()
	if err != nil {
		return err
	}
	return nil
}
