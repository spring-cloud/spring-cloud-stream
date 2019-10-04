/*
 * Copyright 2019-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.streams;

import java.util.Map;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Kafka Streams binder metrics implementation that exports the metrics available
 * through {@link KafkaStreams#metrics()} into a micrometer {@link io.micrometer.core.instrument.MeterRegistry}.
 *
 * @author Soby Chacko
 * @since 3.0.0
 */
public class KafkaStreamsBinderMetrics {

	private KafkaStreams kafkaStreams;

	private final MeterRegistry meterRegistry;

	private MeterBinder meterBinder;

	public KafkaStreamsBinderMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	public void bindTo(MeterRegistry meterRegistry) {
		if (this.meterBinder == null) {
			this.meterBinder = new MeterBinder() {
				@Override
				@SuppressWarnings("unchecked")
				public void bindTo(MeterRegistry registry) {
					if (KafkaStreamsBinderMetrics.this.kafkaStreams != null) {
						final Map<MetricName, ? extends Metric> metrics = KafkaStreamsBinderMetrics.this.kafkaStreams.metrics();

						for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
							final Gauge.Builder<KafkaStreamsBinderMetrics> builder =
									Gauge.builder(sanitize(metric.getKey().group() + "." + metric.getKey().name()), this,
											toDoubleFunction(metric.getValue()));
							final Map<String, String> tags = metric.getKey().tags();
							for (Map.Entry<String, String> tag : tags.entrySet()) {
								builder.tag(tag.getKey(), tag.getValue());
							}
							builder.description(metric.getKey().description())
									.register(meterRegistry);
						}
					}
				}

				ToDoubleFunction toDoubleFunction(Metric metric) {
					return (o) -> {
						if (metric.metricValue() instanceof Number) {
							return (Double) metric.metricValue();
						}
						else {
							return 0.0;
						}
					};
				}
			};
		}
		this.meterBinder.bindTo(this.meterRegistry);
	}

	public void addMetrics(KafkaStreams kafkaStreams) {
		synchronized (KafkaStreamsBinderMetrics.this) {
			this.kafkaStreams = kafkaStreams;
			this.bindTo(this.meterRegistry);
		}
	}

	private static String sanitize(String value) {
		return value.replaceAll("-", ".");
	}
}
