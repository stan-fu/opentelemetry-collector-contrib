// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

const (
	transport = "kafka"
	// TODO: update the following attributes to reflect semconv
	attrInstanceName = "name"
	attrPartition    = "partition"
	attrTopic        = "topic"

	// kafkaMarkMessageCallback is the context key under which the
	// markMessageCallback for the current Kafka message is stored.
	kafkaMarkMessageCallback ctxKey = 1

	// AttrKeyRecvTopic / AttrKeyRecvPartition / AttrKeyRecvOffset are log
	// resource attribute keys used by downstream consumers to correlate logs
	// back to their source Kafka coordinates.
	AttrKeyRecvTopic     = "receiver_topic"
	AttrKeyRecvPartition = "receiver_partition"
	AttrKeyRecvOffset    = "receiver_offset"
)

// ctxKey is the unexported type used for context keys defined in this package
// to avoid collisions with keys defined elsewhere.
type ctxKey int

// markMessageCallback is the callback type stored in the context under
// kafkaMarkMessageCallback. Downstream consumers can call MarkMessage(ctx) to
// trigger this callback once the message has been fully processed, allowing
// the receiver to mark the message as consumed (commit the offset).
type markMessageCallback func()

// MarkMessage retrieves the markMessageCallback associated with ctx and
// invokes it. It is a no-op if ctx does not carry a callback or carries a
// value of an unexpected type. This indirection lets downstream consumers
// (e.g. processors with an asynchronous pipeline) defer offset commits until
// they have actually finished handling the message.
func MarkMessage(ctx context.Context) {
	value := ctx.Value(kafkaMarkMessageCallback)
	if value == nil {
		return
	}
	if cb, ok := value.(markMessageCallback); ok && cb != nil {
		cb()
	}
}

// HandlerHook is an optional plug-in interface that callers can supply via
// WithTraceConsumerGroupHandlerHook / WithMetricConsumerGroupHandlerHook /
// WithLogConsumerGroupHandlerHook. The hook receives the same lifecycle
// callbacks as a sarama.ConsumerGroupHandler, plus Init / Start / Shutdown /
// Ack so it can participate in the receiver lifecycle and observe per-message
// acks.
type HandlerHook interface {
	sarama.ConsumerGroupHandler
	Init(Config, receiver.Settings)
	Start(context.Context, component.Host) error
	Shutdown(context.Context) error
	Ack(topic string, partition, offset int64)
}

var errInvalidInitialOffset = errors.New("invalid initial offset")

// kafkaTracesConsumer uses sarama to consume and handle messages from kafka.
type kafkaTracesConsumer struct {
	config            Config
	consumerGroup     sarama.ConsumerGroup
	nextConsumer      consumer.Traces
	topics            []string
	cancelConsumeLoop context.CancelFunc
	unmarshaler       TracesUnmarshaler
	consumeLoopWG     *sync.WaitGroup

	settings         receiver.Settings
	telemetryBuilder *metadata.TelemetryBuilder

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtraction  bool
	headers           []string
	minFetchSize      int32
	defaultFetchSize  int32
	maxFetchSize      int32

	handlerHook       HandlerHook
	extraUnmarshalers map[string]TracesUnmarshaler
}

// kafkaMetricsConsumer uses sarama to consume and handle messages from kafka.
type kafkaMetricsConsumer struct {
	config            Config
	consumerGroup     sarama.ConsumerGroup
	nextConsumer      consumer.Metrics
	topics            []string
	cancelConsumeLoop context.CancelFunc
	unmarshaler       MetricsUnmarshaler
	consumeLoopWG     *sync.WaitGroup

	settings         receiver.Settings
	telemetryBuilder *metadata.TelemetryBuilder

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtraction  bool
	headers           []string
	minFetchSize      int32
	defaultFetchSize  int32
	maxFetchSize      int32

	handlerHook       HandlerHook
	extraUnmarshalers map[string]MetricsUnmarshaler
}

// kafkaLogsConsumer uses sarama to consume and handle messages from kafka.
type kafkaLogsConsumer struct {
	config            Config
	consumerGroup     sarama.ConsumerGroup
	nextConsumer      consumer.Logs
	topics            []string
	cancelConsumeLoop context.CancelFunc
	unmarshaler       LogsUnmarshaler
	consumeLoopWG     *sync.WaitGroup

	settings         receiver.Settings
	telemetryBuilder *metadata.TelemetryBuilder

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtraction  bool
	headers           []string
	minFetchSize      int32
	defaultFetchSize  int32
	maxFetchSize      int32

	handlerHook       HandlerHook
	customExtractor   CustomExtractor
	extraUnmarshalers map[string]LogsUnmarshaler
}

var (
	_ receiver.Traces  = (*kafkaTracesConsumer)(nil)
	_ receiver.Metrics = (*kafkaMetricsConsumer)(nil)
	_ receiver.Logs    = (*kafkaLogsConsumer)(nil)
)

func newTracesReceiver(config Config, set receiver.Settings, nextConsumer consumer.Traces) (*kafkaTracesConsumer, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &kafkaTracesConsumer{
		config:            config,
		topics:            []string{config.Topic},
		nextConsumer:      nextConsumer,
		consumeLoopWG:     &sync.WaitGroup{},
		settings:          set,
		autocommitEnabled: config.AutoCommit.Enable,
		messageMarking:    config.MessageMarking,
		headerExtraction:  config.HeaderExtraction.ExtractHeaders,
		headers:           config.HeaderExtraction.Headers,
		telemetryBuilder:  telemetryBuilder,
		minFetchSize:      config.MinFetchSize,
		defaultFetchSize:  config.DefaultFetchSize,
		maxFetchSize:      config.MaxFetchSize,
	}, nil
}

func createKafkaClient(ctx context.Context, config Config) (sarama.ConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = config.ClientID
	saramaConfig.Metadata.Full = config.Metadata.Full
	saramaConfig.Metadata.Retry.Max = config.Metadata.Retry.Max
	saramaConfig.Metadata.Retry.Backoff = config.Metadata.Retry.Backoff
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = config.AutoCommit.Enable
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = config.AutoCommit.Interval
	saramaConfig.Consumer.Group.Session.Timeout = config.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = config.HeartbeatInterval
	saramaConfig.Consumer.Fetch.Min = config.MinFetchSize
	saramaConfig.Consumer.Fetch.Default = config.DefaultFetchSize
	saramaConfig.Consumer.Fetch.Max = config.MaxFetchSize
	saramaConfig.ChannelBufferSize = config.ChannelBufferSize
	saramaConfig.Consumer.MaxProcessingTime = config.MaxProcessingTime

	var err error
	if saramaConfig.Consumer.Offsets.Initial, err = toSaramaInitialOffset(config.InitialOffset); err != nil {
		return nil, err
	}
	if config.ResolveCanonicalBootstrapServersOnly {
		saramaConfig.Net.ResolveCanonicalBootstrapServers = true
	}
	if config.ProtocolVersion != "" {
		if saramaConfig.Version, err = sarama.ParseKafkaVersion(config.ProtocolVersion); err != nil {
			return nil, err
		}
	}
	if err := kafka.ConfigureAuthentication(ctx, config.Authentication, saramaConfig); err != nil {
		return nil, err
	}
	return sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
}

func (c *kafkaTracesConsumer) Start(_ context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelConsumeLoop = cancel
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	// extensions take precedence over internal encodings
	if unmarshaler, errExt := loadEncodingExtension[ptrace.Unmarshaler](
		host,
		c.config.Encoding,
	); errExt == nil {
		c.unmarshaler = &tracesEncodingUnmarshaler{
			unmarshaler: *unmarshaler,
			encoding:    c.config.Encoding,
		}
	}
	if unmarshaler, ok := c.extraUnmarshalers[c.config.Encoding]; c.unmarshaler == nil && ok {
		c.unmarshaler = unmarshaler
	}
	if unmarshaler, ok := defaultTracesUnmarshalers()[c.config.Encoding]; c.unmarshaler == nil && ok {
		c.unmarshaler = unmarshaler
	}
	if c.unmarshaler == nil {
		return errUnrecognizedEncoding
	}
	// consumerGroup may be set in tests to inject fake implementation.
	if c.consumerGroup == nil {
		if c.consumerGroup, err = createKafkaClient(ctx, c.config); err != nil {
			return err
		}
	}
	consumerGroup := &tracesConsumerGroupHandler{
		logger:            c.settings.Logger,
		unmarshaler:       c.unmarshaler,
		nextConsumer:      c.nextConsumer,
		ready:             make(chan bool),
		obsrecv:           obsrecv,
		autocommitEnabled: c.autocommitEnabled,
		messageMarking:    c.messageMarking,
		headerExtractor:   &nopHeaderExtractor{},
		telemetryBuilder:  c.telemetryBuilder,
		delegate:          c.handlerHook,
	}
	if c.handlerHook != nil {
		c.handlerHook.Init(c.config, c.settings)
		// Start the hook synchronously before spawning consumeLoop so that any
		// state wired in Start() (e.g. host extensions) is guaranteed to be in
		// place by the time sarama invokes ConsumerGroupHandler.Setup() on the
		// delegate. Doing this after `<-ready` races with the wrapper's Setup
		// (which closes ready *before* calling delegate.Setup), leading to nil
		// dereferences in HandlerHook implementations.
		if err := c.handlerHook.Start(ctx, host); err != nil {
			cancel()
			return err
		}
	}
	if c.headerExtraction {
		consumerGroup.headerExtractor = &headerExtractor{
			logger:  c.settings.Logger,
			headers: c.headers,
		}
	}
	c.consumeLoopWG.Add(1)
	go c.consumeLoop(ctx, consumerGroup)
	<-consumerGroup.ready
	return nil
}

func (c *kafkaTracesConsumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	defer c.consumeLoopWG.Done()
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
			c.settings.Logger.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.settings.Logger.Info("Consumer stopped", zap.Error(ctx.Err()))
			return
		}
	}
}

func (c *kafkaTracesConsumer) Shutdown(ctx context.Context) error {
	if c.cancelConsumeLoop == nil {
		return nil
	}
	if c.handlerHook != nil {
		_ = c.handlerHook.Shutdown(ctx)
	}
	c.cancelConsumeLoop()
	c.consumeLoopWG.Wait()
	if c.consumerGroup == nil {
		return nil
	}
	return c.consumerGroup.Close()
}

func newMetricsReceiver(config Config, set receiver.Settings, nextConsumer consumer.Metrics) (*kafkaMetricsConsumer, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &kafkaMetricsConsumer{
		config:            config,
		topics:            []string{config.Topic},
		nextConsumer:      nextConsumer,
		consumeLoopWG:     &sync.WaitGroup{},
		settings:          set,
		autocommitEnabled: config.AutoCommit.Enable,
		messageMarking:    config.MessageMarking,
		headerExtraction:  config.HeaderExtraction.ExtractHeaders,
		headers:           config.HeaderExtraction.Headers,
		telemetryBuilder:  telemetryBuilder,
		minFetchSize:      config.MinFetchSize,
		defaultFetchSize:  config.DefaultFetchSize,
		maxFetchSize:      config.MaxFetchSize,
	}, nil
}

func (c *kafkaMetricsConsumer) Start(_ context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelConsumeLoop = cancel
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	// extensions take precedence over internal encodings
	if unmarshaler, errExt := loadEncodingExtension[pmetric.Unmarshaler](
		host,
		c.config.Encoding,
	); errExt == nil {
		c.unmarshaler = &metricsEncodingUnmarshaler{
			unmarshaler: *unmarshaler,
			encoding:    c.config.Encoding,
		}
	}
	if unmarshaler, ok := c.extraUnmarshalers[c.config.Encoding]; c.unmarshaler == nil && ok {
		c.unmarshaler = unmarshaler
	}
	if unmarshaler, ok := defaultMetricsUnmarshalers()[c.config.Encoding]; c.unmarshaler == nil && ok {
		c.unmarshaler = unmarshaler
	}
	if c.unmarshaler == nil {
		return errUnrecognizedEncoding
	}
	// consumerGroup may be set in tests to inject fake implementation.
	if c.consumerGroup == nil {
		if c.consumerGroup, err = createKafkaClient(ctx, c.config); err != nil {
			return err
		}
	}
	metricsConsumerGroup := &metricsConsumerGroupHandler{
		logger:            c.settings.Logger,
		unmarshaler:       c.unmarshaler,
		nextConsumer:      c.nextConsumer,
		ready:             make(chan bool),
		obsrecv:           obsrecv,
		autocommitEnabled: c.autocommitEnabled,
		messageMarking:    c.messageMarking,
		headerExtractor:   &nopHeaderExtractor{},
		telemetryBuilder:  c.telemetryBuilder,
		delegate:          c.handlerHook,
	}
	if c.handlerHook != nil {
		c.handlerHook.Init(c.config, c.settings)
		// Start the hook synchronously before spawning consumeLoop so that any
		// state wired in Start() (e.g. host extensions) is guaranteed to be in
		// place by the time sarama invokes ConsumerGroupHandler.Setup() on the
		// delegate. Doing this after `<-ready` races with the wrapper's Setup
		// (which closes ready *before* calling delegate.Setup), leading to nil
		// dereferences in HandlerHook implementations.
		if err := c.handlerHook.Start(ctx, host); err != nil {
			cancel()
			return err
		}
	}
	if c.headerExtraction {
		metricsConsumerGroup.headerExtractor = &headerExtractor{
			logger:  c.settings.Logger,
			headers: c.headers,
		}
	}
	c.consumeLoopWG.Add(1)
	go c.consumeLoop(ctx, metricsConsumerGroup)
	<-metricsConsumerGroup.ready
	return nil
}

func (c *kafkaMetricsConsumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	defer c.consumeLoopWG.Done()
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
			c.settings.Logger.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.settings.Logger.Info("Consumer stopped", zap.Error(ctx.Err()))
			return
		}
	}
}

func (c *kafkaMetricsConsumer) Shutdown(ctx context.Context) error {
	if c.cancelConsumeLoop == nil {
		return nil
	}
	if c.handlerHook != nil {
		_ = c.handlerHook.Shutdown(ctx)
	}
	c.cancelConsumeLoop()
	c.consumeLoopWG.Wait()
	if c.consumerGroup == nil {
		return nil
	}
	return c.consumerGroup.Close()
}

func newLogsReceiver(config Config, set receiver.Settings, nextConsumer consumer.Logs) (*kafkaLogsConsumer, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &kafkaLogsConsumer{
		config:            config,
		topics:            []string{config.Topic},
		nextConsumer:      nextConsumer,
		consumeLoopWG:     &sync.WaitGroup{},
		settings:          set,
		autocommitEnabled: config.AutoCommit.Enable,
		messageMarking:    config.MessageMarking,
		headerExtraction:  config.HeaderExtraction.ExtractHeaders,
		headers:           config.HeaderExtraction.Headers,
		telemetryBuilder:  telemetryBuilder,
		minFetchSize:      config.MinFetchSize,
		defaultFetchSize:  config.DefaultFetchSize,
		maxFetchSize:      config.MaxFetchSize,
	}, nil
}

func (c *kafkaLogsConsumer) Start(_ context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelConsumeLoop = cancel
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	// extensions take precedence over internal encodings
	if unmarshaler, errExt := loadEncodingExtension[plog.Unmarshaler](
		host,
		c.config.Encoding,
	); errExt == nil {
		c.unmarshaler = &logsEncodingUnmarshaler{
			unmarshaler: *unmarshaler,
			encoding:    c.config.Encoding,
		}
	}
	if unmarshaler, ok := c.extraUnmarshalers[c.config.Encoding]; c.unmarshaler == nil && ok {
		c.unmarshaler = unmarshaler
	}
	if unmarshaler, errInt := getLogsUnmarshaler(
		c.config.Encoding,
		defaultLogsUnmarshalers(c.settings.BuildInfo.Version, c.settings.Logger),
	); c.unmarshaler == nil && errInt == nil {
		c.unmarshaler = unmarshaler
	}
	if c.unmarshaler == nil {
		return errUnrecognizedEncoding
	}
	// consumerGroup may be set in tests to inject fake implementation.
	if c.consumerGroup == nil {
		if c.consumerGroup, err = createKafkaClient(ctx, c.config); err != nil {
			return err
		}
	}
	logsConsumerGroup := &logsConsumerGroupHandler{
		logger:            c.settings.Logger,
		unmarshaler:       c.unmarshaler,
		nextConsumer:      c.nextConsumer,
		ready:             make(chan bool),
		obsrecv:           obsrecv,
		autocommitEnabled: c.autocommitEnabled,
		messageMarking:    c.messageMarking,
		headerExtractor:   &nopHeaderExtractor{},
		telemetryBuilder:  c.telemetryBuilder,
		customExtractor:   c.customExtractor,
		delegate:          c.handlerHook,
		cleanupTimeout:    c.config.CleanupTimeout,
	}
	if logsConsumerGroup.customExtractor == nil {
		logsConsumerGroup.customExtractor = &noCustomExtractor{}
	}
	if c.handlerHook != nil {
		c.handlerHook.Init(c.config, c.settings)
		// Start the hook synchronously before spawning consumeLoop so that any
		// state wired in Start() (e.g. host extensions) is guaranteed to be in
		// place by the time sarama invokes ConsumerGroupHandler.Setup() on the
		// delegate. Doing this after `<-ready` races with the wrapper's Setup
		// (which closes ready *before* calling delegate.Setup), leading to nil
		// dereferences in HandlerHook implementations.
		if err := c.handlerHook.Start(ctx, host); err != nil {
			cancel()
			return err
		}
	}
	if c.headerExtraction {
		logsConsumerGroup.headerExtractor = &headerExtractor{
			logger:  c.settings.Logger,
			headers: c.headers,
		}
	}
	c.consumeLoopWG.Add(1)
	go c.consumeLoop(ctx, logsConsumerGroup)
	<-logsConsumerGroup.ready
	return nil
}

func (c *kafkaLogsConsumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	defer c.consumeLoopWG.Done()
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
			c.settings.Logger.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.settings.Logger.Info("Consumer stopped", zap.Error(ctx.Err()))
			return
		}
	}
}

func (c *kafkaLogsConsumer) Shutdown(ctx context.Context) error {
	if c.cancelConsumeLoop == nil {
		return nil
	}
	if c.handlerHook != nil {
		_ = c.handlerHook.Shutdown(ctx)
	}
	c.cancelConsumeLoop()
	c.consumeLoopWG.Wait()
	if c.consumerGroup == nil {
		return nil
	}
	return c.consumerGroup.Close()
}

type tracesConsumerGroupHandler struct {
	id           component.ID
	unmarshaler  TracesUnmarshaler
	nextConsumer consumer.Traces
	ready        chan bool
	readyCloser  sync.Once

	logger *zap.Logger

	obsrecv          *receiverhelper.ObsReport
	telemetryBuilder *metadata.TelemetryBuilder

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtractor   HeaderExtractor

	delegate HandlerHook
}

type metricsConsumerGroupHandler struct {
	id           component.ID
	unmarshaler  MetricsUnmarshaler
	nextConsumer consumer.Metrics
	ready        chan bool
	readyCloser  sync.Once

	logger *zap.Logger

	obsrecv          *receiverhelper.ObsReport
	telemetryBuilder *metadata.TelemetryBuilder

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtractor   HeaderExtractor

	delegate HandlerHook
}

type logsConsumerGroupHandler struct {
	id           component.ID
	unmarshaler  LogsUnmarshaler
	nextConsumer consumer.Logs
	ready        chan bool
	readyCloser  sync.Once

	logger *zap.Logger

	obsrecv          *receiverhelper.ObsReport
	telemetryBuilder *metadata.TelemetryBuilder

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtractor   HeaderExtractor

	customExtractor CustomExtractor
	delegate        HandlerHook

	// consumeWg tracks in-flight messages whose markMessageCallback has not
	// fired yet. Cleanup waits on this WG (bounded by cleanupTimeout) so
	// rebalances and shutdown wait for outstanding acks before commits are
	// finalized. The reset is scoped to readyCloser.Do (i.e. once per handler
	// lifetime) on purpose: per-session reset would lose pending Add(1) from
	// the previous session and the eventual asynchronous Done() would drive
	// the counter negative, panicking the consumer.
	consumeWg      sync.WaitGroup
	cleanupTimeout time.Duration
}

var (
	_ sarama.ConsumerGroupHandler = (*tracesConsumerGroupHandler)(nil)
	_ sarama.ConsumerGroupHandler = (*metricsConsumerGroupHandler)(nil)
	_ sarama.ConsumerGroupHandler = (*logsConsumerGroupHandler)(nil)
)

func (c *tracesConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.readyCloser.Do(func() {
		close(c.ready)
	})
	c.telemetryBuilder.KafkaReceiverPartitionStart.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.Name())))
	return nil
}

func (c *tracesConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	c.telemetryBuilder.KafkaReceiverPartitionClose.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.Name())))
	return nil
}

func (c *tracesConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting consumer group", zap.Int32("partition", claim.Partition()))
	if !c.autocommitEnabled {
		defer session.Commit()
	}
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.logger.Debug("Kafka message claimed",
				zap.String("value", string(message.Value)),
				zap.Time("timestamp", message.Timestamp),
				zap.String("topic", message.Topic))
			if !c.messageMarking.After {
				session.MarkMessage(message, "")
			}

			ctx := c.obsrecv.StartTracesOp(session.Context())
			attrs := attribute.NewSet(
				attribute.String(attrInstanceName, c.id.String()),
				attribute.String(attrTopic, claim.Topic()),
				attribute.String(attrPartition, strconv.Itoa(int(claim.Partition()))),
			)
			c.telemetryBuilder.KafkaReceiverMessages.Add(ctx, 1, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverMessageSize.Record(ctx, int64(len(message.Value)), metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverCurrentOffset.Record(ctx, message.Offset, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverOffsetLag.Record(ctx, claim.HighWaterMarkOffset()-message.Offset-1, metric.WithAttributeSet(attrs))

			traces, err := c.unmarshaler.Unmarshal(message.Value)
			if err != nil {
				c.logger.Error("failed to unmarshal message", zap.Error(err))
				c.telemetryBuilder.KafkaReceiverUnmarshalFailedSpans.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.String())))
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}

			c.headerExtractor.extractHeadersTraces(traces, message)
			spanCount := traces.SpanCount()
			err = c.nextConsumer.ConsumeTraces(session.Context(), traces)
			c.obsrecv.EndTracesOp(ctx, c.unmarshaler.Encoding(), spanCount, err)
			if err != nil {
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			if c.messageMarking.After {
				session.MarkMessage(message, "")
			}
			if !c.autocommitEnabled {
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *metricsConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.readyCloser.Do(func() {
		close(c.ready)
	})
	c.telemetryBuilder.KafkaReceiverPartitionStart.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.Name())))
	return nil
}

func (c *metricsConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	c.telemetryBuilder.KafkaReceiverPartitionClose.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.Name())))
	return nil
}

func (c *metricsConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting consumer group", zap.Int32("partition", claim.Partition()))
	if !c.autocommitEnabled {
		defer session.Commit()
	}
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.logger.Debug("Kafka message claimed",
				zap.String("value", string(message.Value)),
				zap.Time("timestamp", message.Timestamp),
				zap.String("topic", message.Topic))
			if !c.messageMarking.After {
				session.MarkMessage(message, "")
			}

			ctx := c.obsrecv.StartMetricsOp(session.Context())
			attrs := attribute.NewSet(
				attribute.String(attrInstanceName, c.id.String()),
				attribute.String(attrTopic, claim.Topic()),
				attribute.String(attrPartition, strconv.Itoa(int(claim.Partition()))),
			)
			c.telemetryBuilder.KafkaReceiverMessages.Add(ctx, 1, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverMessageSize.Record(ctx, int64(len(message.Value)), metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverCurrentOffset.Record(ctx, message.Offset, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverOffsetLag.Record(ctx, claim.HighWaterMarkOffset()-message.Offset-1, metric.WithAttributeSet(attrs))

			metrics, err := c.unmarshaler.Unmarshal(message.Value)
			if err != nil {
				c.logger.Error("failed to unmarshal message", zap.Error(err))
				c.telemetryBuilder.KafkaReceiverUnmarshalFailedMetricPoints.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.String())))
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			c.headerExtractor.extractHeadersMetrics(metrics, message)

			dataPointCount := metrics.DataPointCount()
			err = c.nextConsumer.ConsumeMetrics(session.Context(), metrics)
			c.obsrecv.EndMetricsOp(ctx, c.unmarshaler.Encoding(), dataPointCount, err)
			if err != nil {
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			if c.messageMarking.After {
				session.MarkMessage(message, "")
			}
			if !c.autocommitEnabled {
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *logsConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.readyCloser.Do(func() {
		close(c.ready)
		c.consumeWg = sync.WaitGroup{}
	})
	c.telemetryBuilder.KafkaReceiverPartitionStart.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.String())))
	if c.delegate != nil {
		return c.delegate.Setup(session)
	}
	return nil
}

func (c *logsConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	c.telemetryBuilder.KafkaReceiverPartitionClose.Add(session.Context(), 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.String())))
	ctx := session.Context()
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			attrs := attribute.NewSet(
				attribute.String(attrInstanceName, c.id.String()),
				attribute.String(attrTopic, topic),
				attribute.String(attrPartition, strconv.Itoa(int(partition))),
			)
			c.telemetryBuilder.KafkaReceiverCurrentOffset.Record(ctx, 0, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverOffsetLag.Record(ctx, 0, metric.WithAttributeSet(attrs))
		}
	}
	// Wait for in-flight messages to be acknowledged via MarkMessage, bounded
	// by cleanupTimeout to avoid blocking indefinitely. cleanupTimeout <= 0
	// disables the wait (preserving upstream best-effort behavior).
	if c.cleanupTimeout > 0 {
		done := make(chan struct{})
		go func() {
			c.consumeWg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(c.cleanupTimeout):
			c.logger.Warn("cleanup timed out waiting for in-flight messages",
				zap.Duration("timeout", c.cleanupTimeout))
		}
	}
	if c.delegate != nil {
		return c.delegate.Cleanup(session)
	}
	return nil
}

func (c *logsConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting consumer group", zap.String("topic", claim.Topic()), zap.Int32("partition", claim.Partition()))
	if c.delegate != nil {
		if err := c.delegate.ConsumeClaim(session, claim); err != nil {
			return err
		}
	}
	if !c.autocommitEnabled {
		defer session.Commit()
	}
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.logger.Debug("Kafka message claimed",
				zap.String("topic", message.Topic),
				zap.Int32("partition", message.Partition),
				zap.String("key", string(message.Key)),
				zap.String("value", string(message.Value)),
				zap.Time("timestamp", message.Timestamp))
			if !c.messageMarking.After {
				session.MarkMessage(message, "")
			}

			ctx := c.obsrecv.StartLogsOp(session.Context())
			attrs := attribute.NewSet(
				attribute.String(attrInstanceName, c.id.String()),
				attribute.String(attrTopic, claim.Topic()),
				attribute.String(attrPartition, strconv.Itoa(int(claim.Partition()))),
			)
			c.telemetryBuilder.KafkaReceiverMessages.Add(ctx, 1, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverMessageSize.Record(ctx, int64(len(message.Value)), metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverCurrentOffset.Record(ctx, message.Offset, metric.WithAttributeSet(attrs))
			c.telemetryBuilder.KafkaReceiverOffsetLag.Record(ctx, claim.HighWaterMarkOffset()-message.Offset-1, metric.WithAttributeSet(attrs))

			logs, err := c.unmarshaler.Unmarshal(message.Value)
			if err != nil {
				c.logger.Error("failed to unmarshal message", zap.Error(err))
				c.telemetryBuilder.KafkaReceiverUnmarshalFailedLogRecords.Add(ctx, 1, metric.WithAttributes(attribute.String(attrInstanceName, c.id.String())))
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			c.headerExtractor.extractHeadersLogs(logs, message)
			// CustomExtractor sees the unmarshaled logs together with the raw
			// message so it can attach receiver-specific attributes before
			// they leave the receiver. customExtractor is guaranteed non-nil
			// by the factory (defaulting to noCustomExtractor).
			c.customExtractor.ExtractLogs(session.Context(), logs, message)

			// Attach the source kafka coordinates to every resource log so
			// downstream consumers (routing processors, exporters with
			// per-partition state, etc.) can correlate records back to their
			// origin without re-parsing the raw message.
			topic := message.Topic
			partition := int64(message.Partition)
			offset := message.Offset
			resourceLogs := logs.ResourceLogs()
			for i := 0; i < resourceLogs.Len(); i++ {
				attributes := resourceLogs.At(i).Resource().Attributes()
				attributes.PutStr(AttrKeyRecvTopic, topic)
				attributes.PutInt(AttrKeyRecvPartition, partition)
				attributes.PutInt(AttrKeyRecvOffset, offset)
			}

			// ackDone guards both the wg.Done and the delegate.Ack so a
			// downstream pipeline that mistakenly invokes the callback more
			// than once cannot drive consumeWg below zero or double-ack.
			var ackDone atomic.Bool
			c.consumeWg.Add(1)
			err = c.nextConsumer.ConsumeLogs(context.WithValue(session.Context(), kafkaMarkMessageCallback, markMessageCallback(func() {
				if ackDone.CompareAndSwap(false, true) {
					defer c.consumeWg.Done()
					if c.delegate != nil {
						c.delegate.Ack(topic, partition, offset)
					}
				}
			})), logs)
			c.obsrecv.EndLogsOp(ctx, c.unmarshaler.Encoding(), logs.LogRecordCount(), err)
			if err != nil {
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			if c.messageMarking.After {
				session.MarkMessage(message, "")
			}
			if !c.autocommitEnabled {
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			c.logger.Info("[shutdown] ConsumeClaim session context done")
			return nil
		}
	}
}

func toSaramaInitialOffset(initialOffset string) (int64, error) {
	switch initialOffset {
	case offsetEarliest:
		return sarama.OffsetOldest, nil
	case offsetLatest:
		fallthrough
	case "":
		return sarama.OffsetNewest, nil
	default:
		return 0, errInvalidInitialOffset
	}
}

// loadEncodingExtension tries to load an available extension for the given encoding.
func loadEncodingExtension[T any](host component.Host, encoding string) (*T, error) {
	extensionID, err := encodingToComponentID(encoding)
	if err != nil {
		return nil, err
	}
	encodingExtension, ok := host.GetExtensions()[*extensionID]
	if !ok {
		return nil, fmt.Errorf("unknown encoding extension %q", encoding)
	}
	unmarshaler, ok := encodingExtension.(T)
	if !ok {
		return nil, fmt.Errorf("extension %q is not an unmarshaler", encoding)
	}
	return &unmarshaler, nil
}

// encodingToComponentID converts an encoding string to a component ID using the given encoding as type.
func encodingToComponentID(encoding string) (*component.ID, error) {
	componentType, err := component.NewType(encoding)
	if err != nil {
		return nil, fmt.Errorf("invalid component type: %w", err)
	}
	id := component.NewID(componentType)
	return &id, nil
}
