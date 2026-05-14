// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/pdata/plog"
)

// CustomExtractor allows extracting additional information from the raw Kafka
// message into the resulting plog.Logs before it is forwarded to the next
// consumer. Implementations should be lightweight, since ExtractLogs is invoked
// for every consumed message on the receiving goroutine.
type CustomExtractor interface {
	// Name returns the unique identifier of the extractor. The name is matched
	// against Config.CustomExtractorName when registering extractors via
	// WithLogsCustomExtractor; an empty name disables matching.
	Name() string

	// ExtractLogs is invoked for every message after it has been unmarshaled
	// into plog.Logs but before it is dispatched to the next consumer.
	ExtractLogs(context.Context, plog.Logs, *sarama.ConsumerMessage)
}

// noCustomExtractor is the default extractor used when no custom extractor is
// configured. It is a no-op implementation that satisfies CustomExtractor.
type noCustomExtractor struct{}

func (n *noCustomExtractor) Name() string { return "" }

func (n *noCustomExtractor) ExtractLogs(context.Context, plog.Logs, *sarama.ConsumerMessage) {
}
