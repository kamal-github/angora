package angora

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/kamal-github/angora/internal/backoff"
	"github.com/streadway/amqp"
)

const (
	projPrefix = "angora"
)

var (
	// ErrInvalidAddr is returned, when invalid amqp address is passed.
	ErrInvalidAddr = fmt.Errorf("%s: invalid amqp address", projPrefix)

	// ErrShuttingDown is an error representation, when amqp connection to Broker
	// shutting down
	ErrShuttingDown = fmt.Errorf("%s: connection to Broker shutting down", projPrefix)

	// ErrAlreadyClosed represents error when amqp connection is already closed.
	ErrAlreadyClosed = fmt.Errorf("%s: connection already closed", projPrefix)
)

// Connection is a RabbitMQ client wrapper with additional capabilities to have the reliable
// Publishing and consumption of messages.
//
//  * Support Reconnecting(with exponential backoff) with RabbitMQ Broker, when it is available again.
//  * PublishConfirm for reliable Publishing and republish, if message cannot be confirmed.
//  * Support Channel pool.
//  * Graceful shutdown of confirm channels (wait for all the confirmations to arrive) and stop taking new publish on shutdown
type Connection struct {
	address string // AMQP connection string
	tlsCfg  *tls.Config

	// a condition which signals when connected to Broker.
	isAlive     *sync.Cond
	connectionM *sync.RWMutex
	connection  *amqp.Connection

	// when application is shutting down, no need to reconnect to Broker
	shutdownCh chan struct{}

	// upon receiving connections closed signal, the client will reconnect.
	notifyClose chan *amqp.Error

	// flag for setting channel pooling
	poolEnabled bool

	// protect reassigning channel pool while reconnecting
	poolM *sync.RWMutex

	// channelPool is a pool of channels of a amqp.Connection
	channelPool *sync.Pool

	// set to true, if confirm publish is required for reliable Publishing
	confirmPublish bool

	// callback function for handing publisher confirm acknowledgement
	onPubConfirm OnPubConfirmFunc

	// manages all the consumer groups
	consumerGroups map[string]*ConsumerGroup
}

// OnPubConfirmFunc is a function type to handle publish confirm acknowledgements
type OnPubConfirmFunc func(deliveryTaggedData interface{}, ack bool)

// NewConnection builds the Connection.
// It takes all the options to build the client to have TLS config, Publish confirm
// @see all the options.
//
// It starts the reconnect task in a separate go routine.
// Also, this starts the reprocessing of failed Publishing in a separate go routines
// asynchronously.
func NewConnection(addr string, opts ...Option) (*Connection, error) {
	_, err := amqp.ParseURI(addr)
	if err != nil {
		return nil, ErrInvalidAddr
	}

	c := &Connection{
		address:        addr,
		shutdownCh:     make(chan struct{}),
		notifyClose:    make(chan *amqp.Error, 1),
		connectionM:    new(sync.RWMutex),
		consumerGroups: make(map[string]*ConsumerGroup),
	}

	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}

	if c.poolEnabled {
		c.poolM = new(sync.RWMutex)
	}

	c.isAlive = sync.NewCond(c.connectionM)

	go c.reconnect()

	c.isAlive.L.Lock()
	for c.connection == nil {
		// waiting until connected to Broker.
		c.isAlive.Wait()
	}
	c.isAlive.L.Unlock()

	return c, nil
}

// Option is a functional option type to build Connection.
type Option func(c *Connection) error

// WithTLSConfig is functional option to build Connection with TLS.
func WithTLSConfig(config *tls.Config) Option {
	return func(c *Connection) error {
		if config == nil {
			return fmt.Errorf("%s: invalid TLS config", projPrefix)
		}
		c.tlsCfg = config
		return nil
	}
}

// WithPublishConfirm is functional option to build Connection with Publish Confirm
// enabled.
func WithPublishConfirm(fn OnPubConfirmFunc) Option {
	return func(c *Connection) error {
		if fn == nil {
			return fmt.Errorf("%s: invalid onPubConfirm", projPrefix)
		}
		c.confirmPublish = true
		c.onPubConfirm = fn
		return nil
	}
}

// EnableChannelPool is functional option to build Connection with Channel Pooling
// enabled.
func EnableChannelPool() Option {
	return func(c *Connection) error {
		c.poolEnabled = true
		return nil
	}
}

// Publish is a wrapper for amqp Publish with resilience.
// It retries to publish in case of connection/channel drop.
func (c *Connection) Publish(ctx context.Context, producerCfg ProducerConfig) (err error) {
	return c.retryOnAMQPClose(ctx, func(ch *channel) error {
		return ch.publish(ctx, producerCfg)
	})
}

// BuildConsumerGroup creates a new ConsumerGroup, applies optional configs and
// register it with the Connection.
func (c *Connection) BuildConsumerGroup(
	name, queue string,
	handler DeliveryHandler,
	concurrencyDegree int,
	consumerCfg ConsumerConfig,
	opts ...ConsumerGroupOption,
) (*ConsumerGroup, error) {
	n := name
	if n == "" {
		n = uuid.New().String()
	}

	if cg, ok := c.consumerGroups[n]; ok {
		return cg, errors.New("consumer group name already in use")
	}

	cg, err := newConsumerGroup(n, queue, handler, concurrencyDegree, consumerCfg, opts...)
	if err != nil {
		return nil, err
	}

	// register consumer group into registry.
	c.consumerGroups[n] = cg

	return cg, nil
}

// CancelConsumerGroup cancels all the consumers running under the given ConsumerGroup.
func (c *Connection) CancelConsumerGroup(ctx context.Context, cg *ConsumerGroup) error {
	if cg == nil {
		return fmt.Errorf("%s: invlid consumer group: %v", projPrefix, cg)
	}

	ch, releaser, err := c.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = releaser()
	}()

	cg.consumersM.Lock()
	defer cg.consumersM.Unlock()
	for i, consumer := range cg.consumers {
		if err := ch.Cancel(string(consumer), false); err != nil {
			return err
		}

		// remove the cancelled consumer from the registry.
		cg.consumers = cg.consumers[i+1:]
	}

	return nil
}

// StartConsumersGroups starts all the consumers (governed by concurrencyDegree) and immediately
// start fetching message and pass it to the handler, in case of channel/connection
// drop, consumers retry to connect and start fetching again.
//
// Message ACK/NACK should be handled as a part of handler implementation.
func (c *Connection) StartConsumersGroups(ctx context.Context) {
	for _, cg := range c.consumerGroups {
		cg := cg

		c.StartConsumersGroup(ctx, cg)
	}
}

// StartConsumersGroup starts given consumer group and immediately
// start fetching message and pass it to the handler, in case of channel/connection
// drop, consumers retry to connect and start fetching again.
//
// Message ACK/NACK should be handled as a part of handler implementation.
func (c *Connection) StartConsumersGroup(ctx context.Context, cg *ConsumerGroup) {
	c.consume(ctx, cg)
}

func (c *Connection) consume(ctx context.Context, cg *ConsumerGroup) {
	for i := 0; i < cg.concurrencyDegree; i++ {
		go func() {
			err := c.retryOnAMQPClose(ctx, func(ch *channel) (err error) {
				if cg.prefetchCount > 0 || cg.prefetchSize > 0 {
					err = ch.Qos(cg.prefetchCount, cg.prefetchSize, cg.prefetchGlobal)
					if err != nil {
						return err
					}
				}

				cName := uuid.New().String()
				shovel, err := ch.Consume(
					cg.queue,
					cName,
					cg.consumerCfg.AutoAck,
					cg.consumerCfg.Exclusive,
					cg.consumerCfg.NoLocal,
					cg.consumerCfg.NoWait,
					cg.consumerCfg.Args,
				)
				if err != nil {
					return fmt.Errorf("%s: failed to consume: %w", projPrefix, err)
				}

				cg.consumersM.Lock()
				cg.consumers = append(cg.consumers, consumer(cName))
				cg.consumersM.Unlock()

				for m := range shovel {
					cg.handler.Handle(ctx, m)
				}

				// if shovel channel is closed, it implies the amqp.Channel/Connection is closed.
				return fmt.Errorf("%s: delivery channel closed: %w", projPrefix, amqp.ErrClosed)
			})
			if err != nil {
				return
			}
		}()
	}
}

// retryOnAMQPClose retries to runs the runnable, when AMQP connection/channel is closed.
// When error is other than amqp.ErrClosed, it returns.
//
// It stops running the runnable when the Connection is in closing state, and returns
// the ErrShuttingDown.
//
// Returns ctx.Err() when context gets canceled.
func (c *Connection) retryOnAMQPClose(ctx context.Context, runnable func(ch *channel) error) (err error) {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.shutdownCh:
			// Stop retrying, while client shutting down.
			return ErrShuttingDown
		default:
			err = c.execRunnable(runnable)
			if errors.Is(err, amqp.ErrClosed) {
				break
			}

			return err
		}
	}
}

// execRunnable gets the channel and runs the runnable, and release the channel
// either to pool/close it (as per pooling enabled/disabled).
func (c *Connection) execRunnable(runnable func(ch *channel) error) error {
	ch, releaseCh, err := c.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = releaseCh()
	}()

	if err := runnable(ch); err != nil {
		return err
	}

	return nil
}

// Shutdown gracefully close the amqp connection and its associated resources.
// Calling Shutdown more than once will panic.
func (c *Connection) Shutdown(ctx context.Context) error {
	// shutdownCh is closed to notify all the reader of this to notify the amqp
	// connection is going to be closed and stop "new" Publishing and
	// reconnecting from this connection.
	close(c.shutdownCh)

	c.connectionM.RLock()
	defer c.connectionM.RUnlock()

	if c.connection == nil || c.connection.IsClosed() {
		return ErrAlreadyClosed
	}

	// connection.Close() will close amqp Connection, all the channels which
	// are created from this connection, consumers, listener channels for
	// publish confirms, blocks, flows etc.
	return c.connection.Close()
}

// reconnect tries to reconnect to RabbitMQ broker as soon as amqp.Connection gets closed.
//
// it stop reconnecting and returns as soon as shutdown is requested by client.
func (c *Connection) reconnect() {
	var retried int

	for {
		if !c.connect() {
			retried++
			backoff.ExponentialWait(retried, 20)
			continue
		}

		c.isAlive.Signal()

		select {
		case <-c.notifyClose:
		case <-c.shutdownCh:
			return
		}
	}
}

// connect opens the connection to Broker, if successful, setups the channel pool.
func (c *Connection) connect() (done bool) {
	if err := c.open(); err != nil {
		return
	}

	if c.poolEnabled {
		c.createChannelPool()
	}

	return true
}

// open dials to RabbitMQ Broker and creates a connection and set it
// to client.Connection.
//
// This also sets up a ConnectionClose notifier.
func (c *Connection) open() error {
	conn, err := amqp.DialTLS(c.address, c.tlsCfg)
	if err != nil {
		return fmt.Errorf("%s: cannot dial amqp connection: %w", projPrefix, err)
	}

	// Note - Create a new chan, as the old chan got closed on RabbitMQ server shutdown
	// so we need to create a new *amqp.Error chan.
	c.notifyClose = make(chan *amqp.Error, 1)

	c.connectionM.Lock()
	defer c.connectionM.Unlock()

	c.connection = conn
	c.connection.NotifyClose(c.notifyClose)

	return nil
}

// createChannelPool sets up a thread safe channel pool.
func (c *Connection) createChannelPool() {
	p := &sync.Pool{
		New: func() interface{} {
			var (
				amqpCh *amqp.Channel
				err    error
			)
			if amqpCh, err = c.amqpChannel(); err != nil {
				return nil
			}
			ch, _ := newChannel(channelParams{
				amqpCh:         amqpCh,
				confirmPublish: c.confirmPublish,
				onPubConfirmFn: c.onPubConfirm,
			})
			return ch
		},
	}

	c.poolM.Lock()
	defer c.poolM.Unlock()
	c.channelPool = p
}

// ChannelReleaser is a function type which should have implementation to release the
// earlier acquired channel.
type ChannelReleaser func() error

// getChannel fetches the channel from pool or create a new one.
//
// Returns error if cannot get channel, also returns a channel releaser closure
// to close/return the channel after use.
func (c *Connection) getChannel() (*channel, ChannelReleaser, error) {
	var err error

	if !c.poolEnabled {
		return c.openChannel()
	}

	c.poolM.RLock()
	ch := c.channelPool.Get()
	c.poolM.RUnlock()

	return ch.(*channel), func() error {
		if ch != nil {
			c.poolM.RLock()
			defer c.poolM.RUnlock()

			c.channelPool.Put(ch)
		}

		return nil
	}, err
}

func (c *Connection) amqpChannel() (*amqp.Channel, error) {
	c.connectionM.RLock()
	defer c.connectionM.RUnlock()

	amqpCh, err := c.connection.Channel()
	if err != nil {
		return nil, err
	}

	return amqpCh, nil
}

func (c *Connection) openChannel() (*channel, ChannelReleaser, error) {
	var (
		amqpCh *amqp.Channel
		err    error
	)

	if amqpCh, err = c.amqpChannel(); err != nil {
		return nil, nil, err
	}

	ch, err := newChannel(channelParams{
		amqpCh:         amqpCh,
		confirmPublish: c.confirmPublish,
		onPubConfirmFn: c.onPubConfirm,
	})
	if err != nil {
		return nil, nil, err
	}

	return ch, func() error {
		return ch.Close()
	}, nil
}

// GetChannel returns the channel reference from the Pool or create a new one wrapping
// *amqp.Channel.
//
// Returns error, if could not get one and ChannelReleaser to return the channel to
// pool or close it.
func (c *Connection) GetChannel() (*channel, ChannelReleaser, error) {
	ch, chCloser, err := c.getChannel()
	if err != nil {
		return nil, nil, err
	}

	return ch, chCloser, nil
}

// GetConnection returns the underlying amqp.Connection reference, but it is recommended
// to use GetChannel method to perform operations on channel.
func (c *Connection) GetConnection() *amqp.Connection {
	c.connectionM.RLock()
	defer c.connectionM.RUnlock()

	return c.connection
}
