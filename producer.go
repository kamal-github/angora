package angora

import "github.com/streadway/amqp"

// ProducerConfig represents RabbitMQ Producer configuration.
type ProducerConfig struct {
	Exchange, RoutingKey string
	Mandatory, Immediate bool
	Publishing           amqp.Publishing
}
