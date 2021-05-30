package angora

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newConsumerGroup(t *testing.T) {
	type args struct {
		name              string
		queue             string
		handler           DeliveryHandler
		concurrencyDegree int
		config            ConsumerConfig
		opts              []ConsumerGroupOption
	}
	tests := []struct {
		name    string
		args    args
		want    *ConsumerGroup
		wantErr bool
	}{
		{
			name: "creates with invalid prefetch params",
			args: args{
				queue:             "test-queue",
				handler:           nil,
				concurrencyDegree: 3,
				config:            ConsumerConfig{},
				opts:              []ConsumerGroupOption{WithPrefetch(0, true)},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "creates with prefetch params",
			args: args{
				queue:             "test-queue",
				handler:           nil,
				concurrencyDegree: 3,
				config: ConsumerConfig{
					AutoAck:   false,
					Exclusive: false,
					NoLocal:   false,
					NoWait:    false,
					Args:      nil,
				},
				opts: []ConsumerGroupOption{WithPrefetch(50, true)},
			},
			want: &ConsumerGroup{
				queue:             "test-queue",
				handler:           nil,
				concurrencyDegree: 3,
				consumersM:        new(sync.Mutex),
				consumers:         make([]consumer, 0),
				prefetchCount:     50,
				prefetchSize:      0,
				prefetchGlobal:    true,
				consumerCfg:       ConsumerConfig{},
			},
			wantErr: false,
		},
		{
			name: "creates default",
			args: args{
				name:              "test-cg",
				queue:             "test-queue",
				handler:           nil,
				concurrencyDegree: 3,
				config: ConsumerConfig{
					AutoAck:   false,
					Exclusive: false,
					NoLocal:   false,
					NoWait:    false,
					Args:      nil,
				},
				opts: nil,
			},
			want: &ConsumerGroup{
				name:              "test-cg",
				queue:             "test-queue",
				handler:           nil,
				concurrencyDegree: 3,
				consumersM:        new(sync.Mutex),
				consumers:         make([]consumer, 0),
				prefetchCount:     0,
				prefetchSize:      0,
				prefetchGlobal:    false,
				consumerCfg:       ConsumerConfig{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newConsumerGroup(tt.args.name, tt.args.queue, tt.args.handler, tt.args.concurrencyDegree, tt.args.config, tt.args.opts...)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.EqualValues(t, tt.want, got)
		})
	}
}
