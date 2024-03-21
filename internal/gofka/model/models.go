package model

type KafkaMessage struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}
