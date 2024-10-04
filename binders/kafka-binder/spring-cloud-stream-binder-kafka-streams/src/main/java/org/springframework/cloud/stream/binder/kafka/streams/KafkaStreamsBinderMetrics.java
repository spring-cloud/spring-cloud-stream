/*
 * Copyright 2019-2024 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToDoubleFunction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Kafka Streams binder metrics implementation that exports the metrics available
 * through {@link KafkaStreams#metrics()} into a micrometer {@link io.micrometer.core.instrument.MeterRegistry}.
 *
 * Boot 2.2 users need to rely on this class for the metrics instead of direct support from Micrometer.
 * Micrometer added Kafka Streams metrics support in 1.4.0 which Boot 2.3 includes.
 * Therefore, the users who are on Boot 2.2, need to rely on these metrics.
 * For users who are on 2.3 Boot, this class won't be activated (See the configuration for the various
 * conditionals used).
 *
 * For the most part, this class is a copy of the Micrometer Kafka Streams support that was added in version 1.4.0.
 * We will keep this class, as long as we support Boot 2.2.x.
 *
 * @author Soby Chacko
 * @author Omer Celik
 * @since 3.0.0
 */
public class KafkaStreamsBinderMetrics {

	static final String DEFAULT_VALUE = "unknown";

	static final String CLIENT_ID_TAG_NAME = "client-id";

	static final String METRIC_GROUP_APP_INFO = "app-info";

	static final String VERSION_METRIC_NAME = "version";

	static final String START_TIME_METRIC_NAME = "start-time-ms";

	static final String KAFKA_VERSION_TAG_NAME = "kafka-version";

	static final String METRIC_NAME_PREFIX = "kafka.";

	static final String METRIC_GROUP_METRICS_COUNT = "kafka-metrics-count";

	private String kafkaVersion = DEFAULT_VALUE;

	private String clientId = DEFAULT_VALUE;

	private final MeterRegistry meterRegistry;

	private MeterBinder meterBinder;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	private volatile Set<MetricName> currentMeters = new HashSet<>();

	private static final ReentrantLock lock = new ReentrantLock();

	public KafkaStreamsBinderMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	public void bindTo(Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans) {
		if (this.meterBinder == null) {
			this.meterBinder = registry -> {
				if (streamsBuilderFactoryBeans != null) {
					for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilderFactoryBeans) {
						if (streamsBuilderFactoryBean.isRunning()) {
							KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
							final Map<MetricName, ? extends Metric> metrics = Objects.requireNonNull(kafkaStreams).metrics();

							prepareToBindMetrics(registry, metrics);
							checkAndBindMetrics(registry, metrics);
						}
					}
				}
			};
		}
		this.meterBinder.bindTo(this.meterRegistry);
	}

	public void addMetrics(Set<StreamsBuilderFactoryBean> streamsBuilderFactoryBeans) {
		try {
			lock.lock();
			this.bindTo(streamsBuilderFactoryBeans);
		}
		finally {
			lock.unlock();
		}
	}

	void prepareToBindMetrics(MeterRegistry registry, Map<MetricName, ? extends Metric> metrics) {
		Metric startTime = null;
		for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
			MetricName name = entry.getKey();
			if (clientId.equals(DEFAULT_VALUE) && name.tags().get(CLIENT_ID_TAG_NAME) != null) {
				clientId = name.tags().get(CLIENT_ID_TAG_NAME);
			}
			if (METRIC_GROUP_APP_INFO.equals(name.group())) {
				if (VERSION_METRIC_NAME.equals(name.name())) {
					kafkaVersion = (String) entry.getValue().metricValue();
				}
				else if (START_TIME_METRIC_NAME.equals(name.name())) {
					startTime = entry.getValue();
				}
			}
		}
		if (startTime != null) {
			bindMeter(registry, startTime, meterName(startTime), meterTags(startTime));
		}
	}

	private void bindMeter(MeterRegistry registry, Metric metric, String name, Iterable<Tag> tags) {
		if (name.endsWith("total") || name.endsWith("count")) {
			registerCounter(registry, metric, name, tags);
		}
		else {
			registerGauge(registry, metric, name, tags);
		}
	}

	private void registerCounter(MeterRegistry registry, Metric metric, String name, Iterable<Tag> tags) {
		FunctionCounter.builder(name, metric, toMetricValue())
				.tags(tags)
				.description(metric.metricName().description())
				.register(registry);
	}

	private ToDoubleFunction<Metric> toMetricValue() {
		return metric -> ((Number) metric.metricValue()).doubleValue();
	}

	private void registerGauge(MeterRegistry registry, Metric metric, String name, Iterable<Tag> tags) {
		Gauge.builder(name, metric, toMetricValue())
				.tags(tags)
				.description(metric.metricName().description())
				.register(registry);
	}

	private List<Tag> meterTags(Metric metric) {
		return meterTags(metric, false);
	}

	private String meterName(Metric metric) {
		String name = METRIC_NAME_PREFIX + metric.metricName().group() + "." + metric.metricName().name();
		return name.replaceAll("-metrics", "").replaceAll("-", ".");
	}

	private List<Tag> meterTags(Metric metric, boolean includeCommonTags) {
		List<Tag> tags = new ArrayList<>();
		metric.metricName().tags().forEach((key, value) -> tags.add(Tag.of(key, value)));
		tags.add(Tag.of(KAFKA_VERSION_TAG_NAME, kafkaVersion));
		return tags;
	}

	private boolean differentClient(List<Tag> tags) {
		for (Tag tag : tags) {
			if (tag.getKey().equals(CLIENT_ID_TAG_NAME)) {
				if (!clientId.equals(tag.getValue())) {
					return true;
				}
			}
		}
		return false;
	}

	void checkAndBindMetrics(MeterRegistry registry, Map<MetricName, ? extends Metric> metrics) {
		if (!currentMeters.equals(metrics.keySet())) {
			currentMeters = new HashSet<>(metrics.keySet());
			metrics.forEach((name, metric) -> {
				//Filter out non-numeric values
				if (!(metric.metricValue() instanceof Number)) {
					return;
				}

				//Filter out metrics from groups that include metadata
				if (METRIC_GROUP_APP_INFO.equals(name.group())) {
					return;
				}
				if (METRIC_GROUP_METRICS_COUNT.equals(name.group())) {
					return;
				}
				String meterName = meterName(metric);
				List<Tag> meterTagsWithCommonTags = meterTags(metric, true);
				//Kafka has metrics with lower number of tags (e.g. with/without topic or partition tag)
				//Remove meters with lower number of tags
				boolean hasLessTags = false;
				for (Meter other : registry.find(meterName).meters()) {
					List<Tag> tags = other.getId().getTags();
					// Only consider meters from the same client before filtering
					if (differentClient(tags)) {
						break;
					}
					if (tags.size() < meterTagsWithCommonTags.size()) {
						registry.remove(other);
					}
						// Check if already exists
					else if (tags.size() == meterTagsWithCommonTags.size()) {
						if (tags.equals(meterTagsWithCommonTags)) {
							return;
						}
						else {
							break;
						}
					}
					else {
						hasLessTags = true;
					}
				}
				if (hasLessTags) {
					return;
				}
				bindMeter(registry, metric, meterName, meterTags(metric));
			});
		}
	}
}
