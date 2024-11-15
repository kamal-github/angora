package angora

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DeliveryHandler is a AMQP message handler.
type DeliveryHandler interface {
	Handle(ctx context.Context, msg amqp.Delivery)
}

// consumer is an identifier type for consumer.
type consumer string

// ConsumerGroup is a consolidator of all the consumers for a queue.
// all the Consumers will be using its own channel to make it thread-safe.
// QOS is governed by Prefetch parameters.
// Message will be requeue or reject as per the preconfigured Errors.
type ConsumerGroup struct {
	name              string
	queue             string
	handler           DeliveryHandler // Message handler, should provide the message parsing and handling of the message
	concurrencyDegree int             // start number of consumers based on this value
	consumersM        *sync.Mutex
	consumers         []consumer

	// Prefetch parameters for Consumers, required to set QOS.
	prefetchCount  int
	prefetchSize   int
	prefetchGlobal bool

	consumerCfg ConsumerConfig
}

// ConsumerGroupOption is a functional option to help setting optional fields.
type ConsumerGroupOption func(c *ConsumerGroup) error

// ConsumerConfig defines all the standard amqp config for consuming messages.
type ConsumerConfig struct {
	AutoAck, Exclusive, NoLocal, NoWait bool
	Args                                amqp.Table
}

func newConsumerGroup(
	name string,
	queue string,
	handler DeliveryHandler,
	concurrencyDegree int,
	config ConsumerConfig,
	opts ...ConsumerGroupOption,
) (*ConsumerGroup, error) {
	g := ConsumerGroup{
		name:              name,
		queue:             queue,
		handler:           handler,
		concurrencyDegree: concurrencyDegree,
		consumerCfg:       config,
		consumersM:        new(sync.Mutex),
		consumers:         make([]consumer, 0),
	}

	for _, o := range opts {
		if err := o(&g); err != nil {
			return nil, err
		}
	}

	return &g, nil
}

// WithPrefetch returns ConsumerGroupOption to set the prefetchCount and prefetchGlobal for a given ConsumerGroup.
func WithPrefetch(prefetchCount int, prefetchGlobal bool) ConsumerGroupOption {
	return func(c *ConsumerGroup) error {
		if prefetchCount <= 0 {
			return fmt.Errorf("%s: invlid prefetch count: %d", projPrefix, prefetchCount)
		}

		c.prefetchCount = prefetchCount
		c.prefetchGlobal = prefetchGlobal

		return nil
	}
}
