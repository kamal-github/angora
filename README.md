[![Go Reference](https://pkg.go.dev/badge/golang.org/x/lint.svg)](https://pkg.go.dev/github.com/kamal-github/angora)
![Github](https://github.com/kamal-github/angora/actions/workflows/ci.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/kamal-github/angora)](https://goreportcard.com/report/github.com/kamal-github/angora)

## angora - A resilient RabbitMQ Go client wrapper.

---
angora supports all the AMQP 0.9.1 client and RabbitMQ extension features with additional resilience features.

## Features
- Reconnect when disconnected from Broker such as when Broker restarts or Connection/Channel close.
- Reliable publishing using publish confirm - configurable.
- Custom failure handling on publish confirm NACK.
- Configurable thread-safe Channel pooling. 
- Supports the graceful shutdown.

## Usage

#### Reliable Publishing
```go
amqpURL := "amqp://host/"

cli, err := angora.NewConnection(amqpURL,
    angora.WithPublishConfirm(), // to enable publish confirm on channel.
    angora.WithTLSConfig(tlsConfig), // to use TLS to connect to Broker
    angora.WithChannelPool(), // to enable AMQP channels pooling
)
// handle error

ctx := context.Background()

pubCfg := angora.ProducerConfig{
    RoutingKey: "post.created",
    Mandatory:  false,
    Immediate:  false,
}

publishing := amqp.Publishing{
    ContentType:  "application/json",
    DeliveryMode: amqp.Transient,
    Timestamp:    time.Now().UTC(),
    Body:         jsonPayload,
    // set other necessary fields.
}

err := cli.Publish(ctx, "exchangeName", pubCfg, publishing)
// handle error
```

#### Reliable and Secure Consuming with ChannelPooling
```go
amqpURL := "amqp://host/"

// Create a angora Connection establishes amqp connection with Broker which reconnects automatically on amqp.Close.
cli, err := angora.NewConnection(amqpURL,
    angora.WithTLSConfig(tlsConfig), // to use TLS to connect to Broker
    angora.WithChannelPool(),        // to enable AMQP channels pooling
)

// Build a ConsumerGroup. 
cg, err := c.BuildConsumerGroup(
    "test-cg",              // consumer name (optional)
    "test-queue",           // RabbitMQ Queue name
    &handler{},             // message delivery handler 
    3,                      // number of concurrent consumer running under group
    angora.ConsumerConfig{  // Consume configuration
        AutoAck:   true,
        Exclusive: false,
        NoLocal:   false,
        NoWait:    false,
        Args:      nil,
    },
)

// Start ConsumerGroup which would in turn start Consumers as per concurrency degree.
cli.StartConsumerGroup(ctx, cg)
```

#### Cancel the ConsumerGroup

```go
cli.CancelConsumerGroup(context.Background(), cg)
```

#### Gracefully shutdown the angora.Connection

This will close the amqp Connection and all the channels, it will stop taking all the new publishing and stop all the running Consumers, if any.

```go
cli.Shutdown()
```

## FAQs
**Q: Why was the consumer group concept added?**  
**A:** The consumer group concept allows you to easily manage (start/stop) all concurrent consumers with a single command, instead of handling them individually.

## Contributing
Create your pull request on a branch other than main. Add test or example to reflect your changes.

This library is covered by the integration tests, before running tests make sure you have RabbitMQ running on local or container (use `docker-compose.yml` to start the rabbitMQ) 

```
source .env
make test
```

Github workflow will also run the integration tests.

## License
This project is licensed under the [MIT license](LICENSE).

