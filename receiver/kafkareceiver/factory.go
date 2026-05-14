// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

const (
	defaultTracesTopic       = "otlp_spans"
	defaultMetricsTopic      = "otlp_metrics"
	defaultLogsTopic         = "otlp_logs"
	defaultEncoding          = "otlp_proto"
	defaultBroker            = "localhost:9092"
	defaultClientID          = "otel-collector"
	defaultGroupID           = defaultClientID
	defaultInitialOffset     = offsetLatest
	defaultSessionTimeout    = 10 * time.Second
	defaultHeartbeatInterval = 3 * time.Second

	// default from sarama.NewConfig()
	defaultMetadataRetryMax = 3
	// default from sarama.NewConfig()
	defaultMetadataRetryBackoff = time.Millisecond * 250
	// default from sarama.NewConfig()
	defaultMetadataFull = true

	// default from sarama.NewConfig()
	defaultAutoCommitEnable = true
	// default from sarama.NewConfig()
	defaultAutoCommitInterval = 1 * time.Second

	// default minimum bytes per fetch from Kafka (default "1")
	defaultMinFetchSize = int32(1)
	// default bytes per fetch from Kafka (default "1048576")
	defaultDefaultFetchSize = int32(1048576)
	// default maximum bytes per fetch from Kafka (default "0", no limit)
	defaultMaxFetchSize = int32(0)

	defaultChannelBufferSize = 1024
	defaultMaxProcessingTime = 512 * time.Millisecond
	defaultCleanupTimeout    = 5 * time.Second
)

var errUnrecognizedEncoding = errors.New("unrecognized encoding")

// FactoryOption applies changes to the kafka receiver factory.
type FactoryOption func(factory *kafkaReceiverFactory)

// WithLogsCustomExtractor registers one or more CustomExtractor implementations
// that will be invoked for every consumed logs message after unmarshalling. The
// extractor whose Name() matches Config.CustomExtractorName is selected at
// runtime; if none matches a no-op extractor is used.
func WithLogsCustomExtractor(extractors ...CustomExtractor) FactoryOption {
	return func(factory *kafkaReceiverFactory) {
		factory.logsExtractors = append(factory.logsExtractors, extractors...)
	}
}

// WithTraceConsumerGroupHandlerHook registers a HandlerHook factory for the
// traces pipeline. The hook is created once per receiver Start and receives
// lifecycle events plus per-message Ack callbacks.
func WithTraceConsumerGroupHandlerHook(hookFactory func() HandlerHook) FactoryOption {
	return func(factory *kafkaReceiverFactory) {
		factory.traceHandlerHook = hookFactory
	}
}

// WithMetricConsumerGroupHandlerHook registers a HandlerHook factory for the
// metrics pipeline.
func WithMetricConsumerGroupHandlerHook(hookFactory func() HandlerHook) FactoryOption {
	return func(factory *kafkaReceiverFactory) {
		factory.metricHandlerHook = hookFactory
	}
}

// WithLogConsumerGroupHandlerHook registers a HandlerHook factory for the logs
// pipeline.
func WithLogConsumerGroupHandlerHook(hookFactory func() HandlerHook) FactoryOption {
	return func(factory *kafkaReceiverFactory) {
		factory.logHandlerHook = hookFactory
	}
}

// NewFactory creates Kafka receiver factory.
func NewFactory(options ...FactoryOption) receiver.Factory {
	f := &kafkaReceiverFactory{}
	for _, o := range options {
		o(f)
	}
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithTraces(f.createTracesReceiver, metadata.TracesStability),
		receiver.WithMetrics(f.createMetricsReceiver, metadata.MetricsStability),
		receiver.WithLogs(f.createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Encoding:          defaultEncoding,
		Brokers:           []string{defaultBroker},
		ClientID:          defaultClientID,
		GroupID:           defaultGroupID,
		InitialOffset:     defaultInitialOffset,
		SessionTimeout:    defaultSessionTimeout,
		HeartbeatInterval: defaultHeartbeatInterval,
		Metadata: kafkaexporter.Metadata{
			Full: defaultMetadataFull,
			Retry: kafkaexporter.MetadataRetry{
				Max:     defaultMetadataRetryMax,
				Backoff: defaultMetadataRetryBackoff,
			},
		},
		AutoCommit: AutoCommit{
			Enable:   defaultAutoCommitEnable,
			Interval: defaultAutoCommitInterval,
		},
		MessageMarking: MessageMarking{
			After:   false,
			OnError: false,
		},
		HeaderExtraction: HeaderExtraction{
			ExtractHeaders: false,
		},
		MinFetchSize:      defaultMinFetchSize,
		DefaultFetchSize:  defaultDefaultFetchSize,
		MaxFetchSize:      defaultMaxFetchSize,
		ChannelBufferSize: defaultChannelBufferSize,
		MaxProcessingTime: defaultMaxProcessingTime,
		CleanupTimeout:    defaultCleanupTimeout,
	}
}

type kafkaReceiverFactory struct {
	logsExtractors    []CustomExtractor
	traceHandlerHook  func() HandlerHook
	metricHandlerHook func() HandlerHook
	logHandlerHook    func() HandlerHook
}

// pickHook returns hookFactory() if non-nil, otherwise returns nil. Receivers
// must tolerate a nil hook.
func pickHook(hookFactory func() HandlerHook) HandlerHook {
	if hookFactory == nil {
		return nil
	}
	return hookFactory()
}

// pickLogsExtractor selects the registered CustomExtractor whose Name matches
// the configured custom_extractor name. When no name is configured or no
// extractor matches, a no-op extractor is returned so callers can always
// dereference the result safely.
func (f *kafkaReceiverFactory) pickLogsExtractor(name string) CustomExtractor {
	if name != "" {
		for _, ex := range f.logsExtractors {
			if ex.Name() == name {
				return ex
			}
		}
	}
	return &noCustomExtractor{}
}

func (f *kafkaReceiverFactory) createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (receiver.Traces, error) {
	oCfg := *(cfg.(*Config))
	if oCfg.Topic == "" {
		oCfg.Topic = defaultTracesTopic
	}

	r, err := newTracesReceiver(oCfg, set, nextConsumer)
	if err != nil {
		return nil, err
	}
	r.handlerHook = pickHook(f.traceHandlerHook)
	return r, nil
}

func (f *kafkaReceiverFactory) createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (receiver.Metrics, error) {
	oCfg := *(cfg.(*Config))
	if oCfg.Topic == "" {
		oCfg.Topic = defaultMetricsTopic
	}

	r, err := newMetricsReceiver(oCfg, set, nextConsumer)
	if err != nil {
		return nil, err
	}
	r.handlerHook = pickHook(f.metricHandlerHook)
	return r, nil
}

func (f *kafkaReceiverFactory) createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	oCfg := *(cfg.(*Config))
	if oCfg.Topic == "" {
		oCfg.Topic = defaultLogsTopic
	}

	r, err := newLogsReceiver(oCfg, set, nextConsumer)
	if err != nil {
		return nil, err
	}
	r.handlerHook = pickHook(f.logHandlerHook)
	r.customExtractor = f.pickLogsExtractor(oCfg.CustomExtractorName)
	return r, nil
}

func getLogsUnmarshaler(encoding string, unmarshalers map[string]LogsUnmarshaler) (LogsUnmarshaler, error) {
	var enc string
	unmarshaler, ok := unmarshalers[encoding]
	if !ok {
		split := strings.SplitN(encoding, "_", 2)
		prefix := split[0]
		if len(split) > 1 {
			enc = split[1]
		}
		unmarshaler, ok = unmarshalers[prefix].(LogsUnmarshalerWithEnc)
		if !ok {
			return nil, errUnrecognizedEncoding
		}
	}

	if unmarshalerWithEnc, ok := unmarshaler.(LogsUnmarshalerWithEnc); ok {
		// This should be called even when enc is an empty string to initialize the encoding.
		unmarshaler, err := unmarshalerWithEnc.WithEnc(enc)
		if err != nil {
			return nil, err
		}
		return unmarshaler, nil
	}

	return unmarshaler, nil
}
