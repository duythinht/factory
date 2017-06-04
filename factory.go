package factory

import (
	"strings"

	"go.uber.org/zap"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	. "github.com/duythinht/functor"
)

type FactoryConfig struct {
	Brokers   string
	GroupName string
	Topics    []string
}

func Config(brokers string, group string, topics string) FactoryConfig {
	return FactoryConfig{
		Brokers:   brokers,
		GroupName: group,
		Topics:    strings.Split(topics, ","),
	}
}

type Factory struct {
	*kafka.Consumer
}

func New(config FactoryConfig) Any {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    config.Brokers,
		"group.id":             config.Brokers,
		"session.timeout.ms":   6000,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		return err
	}

	logger.Info("Created Consumer:", zap.Any("consumer", c))

	if err = c.SubscribeTopics(config.Topics, nil); err != nil {
		return err
	}

	return Factory{c}
}

type WorkerFN func(event Any)

func (fn WorkerFN) Do(event Any) {
	fn(event)
}

type Worker interface {
	Do(a Any)
}

func (f Factory) Run(worker Worker) {
	for {
		event := f.Poll(100) //poll with 100ms
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			logger.Debug("Kafka message received", zap.Any("message", e))
			worker.Do(e)
		case kafka.PartitionEOF:
			logger.Debug("Reached Parition EOF", zap.Any("partion", e))
		case kafka.OffsetsCommitted:
			logger.Debug("Offset commited")
		case kafka.Error:
			logger.Error("Kafka error", zap.Any("error", e))
			worker.Do(e)
			break
		default:
			logger.Warn("Unhandled event", zap.Any("event", e))
		}
	}
	logger.Info("Closing consumer")
	_ = f.Close()
}
