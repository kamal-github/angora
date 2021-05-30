package angora_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kamal-github/angora"
	"github.com/kamal-github/angora/internal"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping, integration test")
	}
}

func TestNewConnection(t *testing.T) {
	type args struct {
		addr string
		opts []angora.Option
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "creates with options",
			args: struct {
				addr string
				opts []angora.Option
			}{
				addr: getURLFromEnv(t),
				opts: []angora.Option{
					angora.EnableChannelPool(),
					angora.WithPublishConfirm(func(p interface{}, ack bool) {}),
					angora.WithTLSConfig(getTestTLSConfig(t)),
				}},
		},
		{
			name: "creates a default angora client",
			args: struct {
				addr string
				opts []angora.Option
			}{
				addr: getURLFromEnv(t),
			},
		},
		{
			name: "returns error with invalid address",
			args: struct {
				addr string
				opts []angora.Option
			}{
				addr: "foobar",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := angora.NewConnection(tt.args.addr, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				defer got.Shutdown(context.TODO())
			}
		})
	}
}

var (
	payload  = []byte("foobar")
	jsonType = "application/json"
)

func TestClientPublish(t *testing.T) {
	integration(t)
	t.Parallel()

	qn, ex, rKey := uniqueStr(t, "test-queue"), uniqueStr(t, "test.exchange"), uniqueStr(t, "test.rKey")
	ch, closer := setup(t, qn, ex, rKey)
	defer closer()

	cli, err := angora.NewConnection(getURLFromEnv(t))
	assert.NoError(t, err)
	defer cli.Shutdown(context.TODO())

	err = cli.Publish(
		context.TODO(),
		angora.ProducerConfig{
			Exchange:   ex,
			RoutingKey: rKey,
			Mandatory:  false,
			Immediate:  false,
			Publishing: amqp.Publishing{
				ContentType:  jsonType,
				Timestamp:    time.Now().UTC(),
				Body:         payload,
				DeliveryMode: amqp.Transient,
			},
		})

	assert.Equal(t, payload, consume(t, ch, qn, true))
}

func TestClientPublish_withReconnectOnConnectionClose(t *testing.T) {
	integration(t)
	//t.Parallel()

	qn, ex, rKey := uniqueStr(t, "test-queue"), uniqueStr(t, "test.exchange"), uniqueStr(t, "test.rKey")
	ch, closer := setup(t, qn, ex, rKey)
	defer closer()

	cli, err := angora.NewConnection(getURLFromEnv(t))
	assert.NoError(t, err)
	defer cli.Shutdown(context.TODO())

	// Closing the connection, to allow it to reconnect.
	cli.GetConnection().Close()

	// Publish will continue to try to publish until connected and publish takes place.
	err = cli.Publish(
		context.TODO(),
		angora.ProducerConfig{
			Exchange:   ex,
			RoutingKey: rKey,
			Mandatory:  false,
			Immediate:  false,
			Publishing: amqp.Publishing{
				ContentType:  jsonType,
				Timestamp:    time.Now().UTC(),
				Body:         payload,
				DeliveryMode: amqp.Transient,
			},
		})

	assert.Equal(t, payload, consume(t, ch, qn, true))
}

func TestClientPublish_withPublishConfirm(t *testing.T) {
	integration(t)
	t.Parallel()

	qn, ex, rKey := uniqueStr(t, "test-queue"), uniqueStr(t, "test.exchange"), uniqueStr(t, "test.rKey")
	ch, closer := setup(t, qn, ex, rKey)
	defer closer()

	onPubConfirmFn := func(p interface{}, ack bool) {
		assert.NotNil(t, p)
		assert.True(t, ack)
	}

	cli, err := angora.NewConnection(getURLFromEnv(t), angora.WithPublishConfirm(onPubConfirmFn))
	assert.NoError(t, err)
	defer cli.Shutdown(context.TODO())

	err = cli.Publish(
		context.TODO(),
		angora.ProducerConfig{
			Exchange:   ex,
			RoutingKey: rKey,
			Mandatory:  false,
			Immediate:  false,
			Publishing: amqp.Publishing{
				ContentType:  jsonType,
				Timestamp:    time.Now().UTC(),
				Body:         payload,
				DeliveryMode: amqp.Transient,
			},
		})

	assert.NoError(t, err)
	assert.Equal(t, payload, consume(t, ch, qn, true))
}

func TestClientPublish_withPublishConfirmAndChannelPool(t *testing.T) {
	integration(t)
	t.Parallel()

	qn, ex, rKey := uniqueStr(t, "test-queue"), uniqueStr(t, "test.exchange"), uniqueStr(t, "test.rKey")
	ch, closer := setup(t, qn, ex, rKey)
	defer closer()

	onPubConfirmFn := func(p interface{}, ack bool) {
		assert.NotNil(t, p)
		assert.True(t, ack)
	}

	cli, err := angora.NewConnection(getURLFromEnv(t), angora.EnableChannelPool(), angora.WithPublishConfirm(onPubConfirmFn))
	assert.NoError(t, err)
	defer cli.Shutdown(context.TODO())

	err = cli.Publish(
		context.TODO(),
		angora.ProducerConfig{
			Exchange:   ex,
			RoutingKey: rKey,
			Mandatory:  false,
			Immediate:  false,
			Publishing: amqp.Publishing{
				ContentType:  jsonType,
				Timestamp:    time.Now().UTC(),
				Body:         payload,
				DeliveryMode: amqp.Transient,
			},
		})

	assert.NoError(t, err)
	assert.Equal(t, payload, consume(t, ch, qn, true))
}

func TestClientConsume(t *testing.T) {
	integration(t)

	type test struct {
		name        string
		client      *angora.Connection
		qn          string
		ex          string
		rKey        string
		concurrency int
	}

	tests := []test{
		{
			name: "No channel pooling",
			client: func() *angora.Connection {
				c, err := angora.NewConnection(getURLFromEnv(t))
				assert.NoError(t, err)
				return c
			}(),
			qn:          uniqueStr(t, "test-queue"),
			ex:          uniqueStr(t, "test.exchange"),
			rKey:        uniqueStr(t, "test.rKey"),
			concurrency: 4,
		},
		{
			name: "With channel pooling",
			client: func() *angora.Connection {
				c, err := angora.NewConnection(getURLFromEnv(t), angora.EnableChannelPool())
				assert.NoError(t, err)
				return c
			}(),
			qn:          uniqueStr(t, "test-queue"),
			ex:          uniqueStr(t, "test.exchange"),
			rKey:        uniqueStr(t, "test.rKey"),
			concurrency: 4,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, closer := setup(t, tc.qn, tc.ex, tc.rKey)
			defer closer()

			cli := tc.client
			defer cli.Shutdown(context.TODO())

			h := &handler{consumed: make(chan struct{})}
			_, err := cli.BuildConsumerGroup(
				"",
				tc.qn,
				h,
				tc.concurrency,
				angora.ConsumerConfig{},
				angora.WithPrefetch(1, false),
			)
			assert.NoError(t, err)

			go cli.StartConsumersGroups(context.TODO())

			for i := 0; i < 100; i++ {
				err := cli.Publish(
					context.TODO(),
					angora.ProducerConfig{
						Exchange:   tc.ex,
						RoutingKey: tc.rKey,
						Mandatory:  false,
						Immediate:  false,
						Publishing: amqp.Publishing{
							ContentType:  jsonType,
							Timestamp:    time.Now().UTC(),
							Body:         payload,
							DeliveryMode: amqp.Transient,
						},
					})
				assert.NoError(t, err)
			}

		loop:
			for {
				select {
				case <-h.consumed:
				case <-time.After(time.Second):
					break loop
				}
			}

			assert.EqualValues(t, 100, atomic.LoadInt32(&h.Cnt))
		})
	}
}

func TestClientCancelConsumerGroup(t *testing.T) {
	type args struct {
		ctx       context.Context
		cgName    string
		getClient func() (*angora.Connection, *angora.ConsumerGroup, func(ctx context.Context) error)
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "cancels consumer group",
			wantErr: false,
			args: args{
				ctx:    context.TODO(),
				cgName: "test-cg",
				getClient: func() (*angora.Connection, *angora.ConsumerGroup, func(ctx context.Context) error) {
					c, err := angora.NewConnection(getURLFromEnv(t))
					assert.NoError(t, err)
					qn, ex, rKey := uniqueStr(t, "test-queue"), uniqueStr(t, "test.exchange"), uniqueStr(t, "test.rKey")
					_, closer := setup(t, qn, ex, rKey)

					cg, err := c.BuildConsumerGroup(
						"test-cg",
						"test-queue",
						&handler{
							consumed: make(chan struct{}),
						},
						3,
						angora.ConsumerConfig{},
					)
					assert.NoError(t, err)

					c.StartConsumersGroups(context.TODO())

					return c, cg, func(ctx context.Context) error {
						c.Shutdown(ctx)
						return closer()
					}
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, cg, closer := tt.args.getClient()
			defer closer(context.TODO())

			// wait for all the consumers to be ready.
			time.Sleep(1 * time.Second)
			assert.Equal(t, tt.wantErr, c.CancelConsumerGroup(tt.args.ctx, cg) != nil)
		})
	}
}

func TestClientGetAMQPChannel(t *testing.T) {
	tests := []struct {
		name           string
		wantErr        bool
		wantChReleaser angora.ChannelReleaser
	}{
		{
			name:    "Returns the AMQP channel",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := angora.NewConnection(getURLFromEnv(t))
			assert.NoError(t, err)
			defer c.Shutdown(context.TODO())

			ch, chCloser, err := c.GetChannel()
			assert.Equal(t, tt.wantErr, err != nil)
			assert.NotNil(t, chCloser)
			defer chCloser()

			assert.NotNil(t, ch)
		})
	}
}

/*
goos: darwin
goarch: amd64
pkg: github.com/kamal-github/angora
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkClientStartConsumersGroupsWithoutPooling
BenchmarkClientStartConsumersGroupsWithoutPooling-8   	     319	   3600053 ns/op
*/
func BenchmarkClientStartConsumersGroupsWithoutPooling(b *testing.B) {
	qn, ex, rKey := uniqueStr(b, "bench-queue"), uniqueStr(b, "bench.exchange"), uniqueStr(b, "bench.rKey")
	_, closer := setup(b, qn, ex, rKey)
	defer closer()

	cli, err := angora.NewConnection(getURLFromEnv(b))
	assert.NoError(b, err)
	defer cli.Shutdown(context.TODO())

	h := &handler{}
	_, err = cli.BuildConsumerGroup(
		"",
		qn,
		h,
		3,
		angora.ConsumerConfig{},
		angora.WithPrefetch(1, false),
	)
	assert.NoError(b, err)

	//cli.RegisterConsumerGroups(cg)
	go cli.StartConsumersGroups(context.TODO())

	b.StopTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = cli.Publish(
			context.TODO(),
			angora.ProducerConfig{
				Exchange:   ex,
				RoutingKey: rKey,
				Mandatory:  false,
				Immediate:  false,
				Publishing: amqp.Publishing{
					ContentType:  jsonType,
					Timestamp:    time.Now().UTC(),
					Body:         payload,
					DeliveryMode: amqp.Transient,
				},
			})
		assert.NoError(b, err)
	}
}

/*
goos: darwin
goarch: amd64
pkg: github.com/kamal-github/angora
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkClientStartConsumersGroupsWithPoolingEnabled
BenchmarkClientStartConsumersGroupsWithPoolingEnabled-8   	   16350	     63752 ns/op
*/
func BenchmarkClientStartConsumersGroupsWithPoolingEnabled(b *testing.B) {
	qn, ex := "bench-client-consume-channel-pool-queue", "bench-client-consume-channel-pool-Exchange"
	qn, ex, rKey := uniqueStr(b, "bench-queue"), uniqueStr(b, "bench.exchange"), uniqueStr(b, "bench.rKey")
	_, closer := setup(b, qn, ex, rKey)
	defer closer()

	cli, err := angora.NewConnection(getURLFromEnv(b), angora.EnableChannelPool())
	assert.NoError(b, err)
	defer cli.Shutdown(context.TODO())

	h := &handler{}
	_, err = cli.BuildConsumerGroup(
		"",
		qn,
		h,
		3,
		angora.ConsumerConfig{},
		angora.WithPrefetch(1, false),
	)
	assert.NoError(b, err)

	go cli.StartConsumersGroups(context.TODO())

	b.StopTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = cli.Publish(
			context.TODO(),
			angora.ProducerConfig{
				Exchange:   ex,
				RoutingKey: rKey,
				Mandatory:  false,
				Immediate:  false,
				Publishing: amqp.Publishing{
					ContentType:  jsonType,
					Timestamp:    time.Now().UTC(),
					Body:         payload,
					DeliveryMode: amqp.Transient,
				},
			})
		assert.NoError(b, err)
	}
}

type handler struct {
	Cnt      int32
	consumed chan struct{}
}

func (h *handler) Handle(ctx context.Context, msg amqp.Delivery) {
	_ = msg.Ack(false)
	h.consumed <- struct{}{}
	atomic.AddInt32(&h.Cnt, 1)
}

func setup(t testing.TB, qn, ex, rKey string) (*amqp.Channel, func() error) {
	conn := newConnection(t)
	ch, err := conn.Channel()
	assert.NoError(t, err)
	declareExchange(t, ch, ex)
	q, closeFn := declareQueue(t, ch, qn)
	assert.NoError(t, ch.QueueBind(q.Name, rKey, ex, false, nil))

	return ch, func() error {
		closeFn()
		return conn.Close()
	}
}

func declareExchange(t testing.TB, ch *amqp.Channel, ex string) {
	assert.NoError(t, ch.ExchangeDeclare(ex, amqp.ExchangeTopic, true, false, false, false, nil))
}

func declareQueue(t testing.TB, ch *amqp.Channel, qn string) (amqp.Queue, func()) {
	q, err := ch.QueueDeclare(qn, false, true, false, false, nil)
	assert.NoError(t, err)

	return q, func() {
		_, err := ch.QueueDelete(q.Name, false, false, false)
		assert.NoError(t, err)
	}
}

func consume(t *testing.T, ch *amqp.Channel, q string, ack bool) []byte {
	delCh, err := ch.Consume(q, "", false, false, false, false, nil)
	assert.NoError(t, err)
	del := <-delCh
	if ack {
		assert.NoError(t, del.Ack(false))
	} else {
		assert.NoError(t, del.Nack(false, false))
	}
	return del.Body
}

func newConnection(t testing.TB) *amqp.Connection {
	t.Helper()

	conn, err := amqp.Dial(getURLFromEnv(t))
	assert.NoError(t, err)

	return conn
}

func getURLFromEnv(tb testing.TB) string {
	tb.Helper()
	return os.Getenv("AMQP_URL")
}

func getTestTLSConfig(tb testing.TB) *tls.Config {
	tb.Helper()

	cert, err := tls.X509KeyPair(internal.LocalhostCert, internal.LocalhostKey)
	if err != nil {
		tb.Fatal(fmt.Sprintf(" %v", err))
	}
	c := new(tls.Config)
	c.NextProtos = []string{"http/1.1"}
	c.Certificates = []tls.Certificate{cert}
	certificate, err := x509.ParseCertificate(c.Certificates[0].Certificate[0])
	if err != nil {
		tb.Fatal(fmt.Sprintf("getTestTLSConfig: %v", err))
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(certificate)
	c.RootCAs = certPool
	return c
}

func uniqueStr(t testing.TB, s string) string {
	t.Helper()

	return fmt.Sprintf("%s-%d", s, time.Now().Nanosecond())
}
