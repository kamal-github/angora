package angora

import amqp "github.com/rabbitmq/amqp091-go"

// ProducerConfig represents RabbitMQ Producer configuration.
type ProducerConfig struct {
	Exchange, RoutingKey string
	Mandatory, Immediate bool
	Publishing           amqp.Publishing
}
