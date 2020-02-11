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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Kafka Streams binder metrics implementation that exports the metrics available
 * through {@link KafkaStreams#metrics()} into a micrometer {@link io.micrometer.core.instrument.MeterRegistry}.
 *
 * @author Soby Chacko
 * @since 3.0.0
 */
public class KafkaStreamsBinderMetrics {

	private final MeterRegistry meterRegistry;

	private MeterBinder meterBinder;

	public KafkaStreamsBinderMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	public void bindTo(Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans, MeterRegistry meterRegistry) {

		if (this.meterBinder == null) {
			this.meterBinder = new MeterBinder() {
				@Override
				@SuppressWarnings("unchecked")
				public void bindTo(MeterRegistry registry) {
					if (streamsBuilderFactoryBeans != null) {
						for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilderFactoryBeans) {
							KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
							final Map<MetricName, ? extends Metric> metrics = kafkaStreams.metrics();

							Set<String> meterNames = new HashSet<>();

							for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
								final String sanitized = sanitize(metric.getKey().group() + "." + metric.getKey().name());
								final String applicationId = streamsBuilderFactoryBean.getStreamsConfiguration().getProperty(StreamsConfig.APPLICATION_ID_CONFIG);

								final String name = streamsBuilderFactoryBeans.size() > 1 ? applicationId + "." + sanitized : sanitized;

								final Gauge.Builder<KafkaStreamsBinderMetrics> builder =
										Gauge.builder(name, this,
												toDoubleFunction(metric.getValue()));
								final Map<String, String> tags = metric.getKey().tags();
								for (Map.Entry<String, String> tag : tags.entrySet()) {
									builder.tag(tag.getKey(), tag.getValue());
								}
								if (!meterNames.contains(name)) {
									builder.description(metric.getKey().description())
											.register(meterRegistry);
									meterNames.add(name);
								}
							}
						}

					}
				}

				ToDoubleFunction toDoubleFunction(Metric metric) {
					return (o) -> {
						if (metric.metricValue() instanceof Number) {
							return ((Number) metric.metricValue()).doubleValue();
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

	private static String sanitize(String value) {
		return value.replaceAll("-", ".");
	}

	public void addMetrics(Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans) {
		synchronized (KafkaStreamsBinderMetrics.this) {
			this.bindTo(streamsBuilderFactoryBeans, this.meterRegistry);
		}
	}
}
