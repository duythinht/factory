package main

import (
	"fmt"
	"os"
	"reflect"
	"sync"

	"go.uber.org/zap"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	schemaregistry "github.com/datamountaineer/schema-registry"
	"github.com/duythinht/factory"
	. "github.com/duythinht/functor"
	"github.com/duythinht/zaptor"
)

var logger = zaptor.GetLogger("factory").WithLevelBy("FACTORY_LOG_LEVEL")

func raise(err error) {
	logger.Error("Runtime error", zap.Error(err))
}

func getSchemaRegistryClient(_url Any) Maybe {
	switch url := _url.(type) {
	case string:
		client, err := schemaregistry.NewClient(url)
		if err != nil {
			raise(err)
			return Nothing{}
		}
		logger.Info("Created a schema registry client")
		return Just(client)
	default:
		logger.Error("invalid schema url")
		return Nothing{}
	}
}

func buildGetSchema(_client Any) Maybe {
	switch client := _client.(type) {
	case schemaregistry.Client:
		schemas := make(map[int]string)
		mutex := &sync.Mutex{}

		return Just(func(a Any) Maybe {
			mutex.Lock()
			defer mutex.Unlock()

			id := a.(int)

			if schema, ok := schemas[id]; ok {
				return Just(schema)
			}

			schema, err := client.GetSchemaById(id)

			if err != nil {
				raise(err)
				return Nothing{}
			}

			schemas[id] = schema

			return Just(schema)
		})
	default:
		return Nothing{}
	}
}

func buildWorker(_getSchema Any) Maybe {
	switch getSchemaById := _getSchema.(type) {
	case func(Any) Maybe:
		return Just(func(event Any) {
			switch message := event.(type) {
			case error:
				raise(message)
			case *kafka.Message:
				fmt.Println(string(message.Value))
				//val := Just(message.Value)
			default:
				logger.Warn("Unhandled message type", zap.Any("type", reflect.TypeOf(message)))
			}
		})
	default:
		logger.Error("Error type", zap.Any("getschema", getSchemaById))
		return Nothing{}
	}
}

func buildFactory(_cfg Any) Maybe {
	cfg := _cfg.(factory.FactoryConfig)
	switch fact := factory.New(cfg).(type) {
	case factory.Factory:
		return Just(fact)
	case error:
		raise(fact)
		return Nothing{}
	default:
		return Nothing{}
	}
}

func applyWorker(_worker Any) Maybe {
	switch w := _worker.(type) {
	case func(Any):

		fn := factory.WorkerFN(w)

		return Just(F1(func(maybeFactory Any) Maybe {
			switch f := maybeFactory.(type) {
			case factory.Factory:
				f.Run(fn)
				return Just(1)
			default:
				return Nothing{}
			}
		}))
	default:
		logger.Error("Not a worker FN", zap.Any("worker", w))
		return Nothing{}
	}
}

func main() {
	broker := "localhost"
	group := "test"
	topics := "hello"
	//getSchema := GetSchemaWith("http://localhost")
	registryURL := "http://localhost"

	switch apply := Just(registryURL).Map(getSchemaRegistryClient).Map(buildGetSchema).Map(buildWorker).Map(applyWorker).(type) {
	case Some:
		cfg := factory.Config(broker, group, topics)
		f := Just(cfg).Map(buildFactory)
		fn := apply.Value.(F1)
		Just("hello").Map(fn)
		f.Map(fn)
	case Nothing:
		os.Exit(1)
	}
}
