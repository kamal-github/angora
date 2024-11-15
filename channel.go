package angora

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// confirmChCap is the buffer capacity for a publish confirmation channel.
const confirmChCap = 1

// channel is threads-safe and is only held by one publisher at a time.
// It is an extended amqp channel, which manages the asynchronous publish confirm.
//
// Thread safety is achieved through channel pool.
type channel struct {
	*amqp.Channel

	// flag to decide whether to set channel in confirm mode.
	confirmPublish bool

	// Callback function when pub confirm with ACK arrives.
	onPubConfirmFn OnPubConfirmFunc

	// buffered channel for reading amqp.Confirmation after publish.
	notifyConfirmCh chan amqp.Confirmation

	// stores the messages by deliveryTag to track unconfirmed messages from Server.
	unconfirmedMsgTracker *sync.Map

	// number of messages published for a given channel.
	published uint64
}

type channelParams struct {
	amqpCh         *amqp.Channel
	confirmPublish bool
	onPubConfirmFn OnPubConfirmFunc
}

// newChannel creates a new amqp channel.
// Registers notifyClose channel and if enabled, notifyPublish channel.
func newChannel(params channelParams) (ch *channel, err error) {
	ch = &channel{
		Channel:        params.amqpCh,
		confirmPublish: params.confirmPublish,
	}

	if ch.confirmPublish {
		if err = params.amqpCh.Confirm(false); err != nil {
			return nil, fmt.Errorf("%s: failed to put channel to publish confirm: %w", projPrefix, err)
		}

		ch.onPubConfirmFn = params.onPubConfirmFn
		ch.notifyConfirmCh = make(chan amqp.Confirmation, confirmChCap)

		ch.NotifyPublish(ch.notifyConfirmCh)

		// This is for tracking unconfirmed published messages, if ack=false
		// returned by Broker, messages can be acted upon such as republish.
		ch.unconfirmedMsgTracker = new(sync.Map)

		go ch.asyncPubConfirmReader()
	}

	return
}

// publish publishes the message to Exchange based on ProducerConfig.
// if channel is in confirm mode, it gets the next Publishing seq number and
// use it to store the unconfirmed payload to reprocess in case, broker fails and
// message gets undelivered.
func (ch *channel) publish(ctx context.Context, producerCfg ProducerConfig) error {
	var n uint64

	if ch.confirmPublish {
		n = ch.nextPublishSeqNo()

		ch.unconfirmedMsgTracker.Store(n, UnconfirmedPub{
			ProducerCfg: producerCfg,
		})
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if err := ch.Publish(
			producerCfg.Exchange,
			producerCfg.RoutingKey,
			producerCfg.Mandatory,
			producerCfg.Immediate,
			producerCfg.Publishing,
		); err != nil {
			// Deleting as it is failed to publish immediately and there will not be
			// any pub confirm with NACK, so clean up the message tracker.
			ch.unconfirmedMsgTracker.Delete(n)

			return fmt.Errorf(
				"%s: channel failed to publish message: %d Exchange: %s routingKey: %s:  %w",
				projPrefix, n, producerCfg.Exchange, producerCfg.RoutingKey, err,
			)
		}
	}

	return nil
}

// asyncPubConfirmReader should be run asynchronously to listen for confirmation for
// published payload, notifyConfirmCh is closed if the server does not support
// confirm mode on channel and in this case, this function will just return.
func (ch *channel) asyncPubConfirmReader() {
	for conf := range ch.notifyConfirmCh {
		trackedPayload, ok := ch.unconfirmedMsgTracker.Load(conf.DeliveryTag)
		if !ok {
			// if DeliveryTag is not added for any reason, just continue.
			continue
		}

		// Call the callback handler for processing the delivery ACK
		ch.onPubConfirmFn(trackedPayload, conf.Ack)

		// delete from the tracking map of unconfirmed payload.
		ch.unconfirmedMsgTracker.Delete(conf.DeliveryTag)
	}
}

// nextPublishSeqNo generates the next Publishing sequence number, it is always
// incremented by 1 for a given channel.
//
// when PR https://github.com/streadway/amqp/pull/478 is merged, this method can
// be removed.
func (ch *channel) nextPublishSeqNo() uint64 {
	return atomic.AddUint64(&ch.published, 1)
}
