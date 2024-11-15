package angora

import amqp "github.com/rabbitmq/amqp091-go"

// ExchangeConfig is RabbitMQ Exchange configuration.
type ExchangeConfig struct {
	Name, Kind                            string
	Durable, AutoDelete, Internal, NoWait bool
	Args                                  amqp.Table
}
