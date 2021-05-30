package angora

import "github.com/streadway/amqp"

// ExchangeConfig is RabbitMQ Exchange configuration.
type ExchangeConfig struct {
	Name, Kind                            string
	Durable, AutoDelete, Internal, NoWait bool
	Args                                  amqp.Table
}
