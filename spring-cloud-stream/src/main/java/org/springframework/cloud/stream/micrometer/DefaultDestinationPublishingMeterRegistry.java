/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.micrometer;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionCounter;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionTimer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultLongTaskTimer;
import io.micrometer.core.instrument.internal.DefaultMeter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.support.GenericMessage;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.0
 *
 */
class DefaultDestinationPublishingMeterRegistry extends MeterRegistry implements SmartLifecycle {

	private static final Log logger = LogFactory.getLog(DefaultDestinationPublishingMeterRegistry.class);

	private final MetricsPublisherConfig metricsPublisherConfig;

	private final Consumer<String> metricsConsumer;

	private final DecimalFormat format = new DecimalFormat("#.####");

	private final ApplicationMetricsProperties applicationProperties;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private ScheduledFuture<?> publisher;

	DefaultDestinationPublishingMeterRegistry(ApplicationMetricsProperties applicationProperties,
			MetersPublisherBinding publisherBinding, MetricsPublisherConfig metricsPublisherConfig, Clock clock) {
		super(clock);
		this.metricsPublisherConfig = metricsPublisherConfig;
		this.metricsConsumer = new MessageChannelPublisher(publisherBinding);
		this.applicationProperties = applicationProperties;
	}

	@Override
	public void start() {
		start(Executors.defaultThreadFactory());
	}

	@Override
	public void stop() {
		if (publisher != null) {
			publisher.cancel(false);
			publisher = null;
		}
	}

	@Override
	public boolean isRunning() {
		return this.publisher != null;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}

	@Override
	protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> f) {
		return new DefaultGauge<>(id, obj, f);
	}

	@Override
	protected Counter newCounter(Meter.Id id) {
		return new CumulativeCounter(id);
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Meter.Id id) {
		return new DefaultLongTaskTimer(id, clock);
	}

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}

	protected void publish() {
		List<Map<String, Object>> aggregatedMeters = new ArrayList<>();
		for (Meter meter : this.getMeters()) {
			if (meter instanceof Timer) {
				aggregatedMeters.add(timerMap((Timer) meter));
			} else if (meter instanceof DistributionSummary) {
				aggregatedMeters.add(summaryMap((DistributionSummary) meter));
			}
		}
		Map<String, Object> messageMap = new LinkedHashMap<>();
		messageMap.put("name", this.applicationProperties.getKey());
		messageMap.put("properties", this.applicationProperties.getExportProperties());
		messageMap.put("meter-snapshots", aggregatedMeters);
		try {
			String jsonString = this.objectMapper.writeValueAsString(messageMap);
			this.metricsConsumer.accept(jsonString);
		} catch (JsonProcessingException e) {
			logger.warn("Error producing JSON String representation metric data", e);
		}
	}

	@Override
	protected Timer newTimer(Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		return new CumulativeTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit());
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Id id, T obj, ToLongFunction<T> countFunction,
			ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnits) {
		return new CumulativeFunctionTimer<T>(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnits,
				getBaseTimeUnit());
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Id id, T obj, ToDoubleFunction<T> valueFunction) {
		return new CumulativeFunctionCounter<T>(id, obj, valueFunction);
	}

	@Override
	protected Meter newMeter(Id id, Type type, Iterable<Measurement> measurements) {
		return new DefaultMeter(id, type, measurements);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Id id, DistributionStatisticConfig distributionStatisticConfig,
			double scale) {
		return new CumulativeDistributionSummary(id, clock, distributionStatisticConfig, scale);
	}

	@Override
	protected DistributionStatisticConfig defaultHistogramConfig() {
		return DistributionStatisticConfig.builder().expiry(metricsPublisherConfig.step()).build()
				.merge(DistributionStatisticConfig.DEFAULT);
	}

	private void start(ThreadFactory threadFactory) {
		if (publisher != null) {
			stop();
		}
		publisher = Executors.newSingleThreadScheduledExecutor(threadFactory).scheduleAtFixedRate(this::publish,
				metricsPublisherConfig.step().toMillis(), metricsPublisherConfig.step().toMillis(),
				TimeUnit.MILLISECONDS);
	}

	private Map<String, Object> summaryMap(DistributionSummary summary) {
		List<Field> fields = this.gatherSnapshotFields(summary.takeSnapshot(false));
		return this.toMeterMap(fields, summary.getId());
	}

	private Map<String, Object> timerMap(Timer timer) {
		List<Field> fields = this.gatherSnapshotFields(timer.takeSnapshot(false));
		return this.toMeterMap(fields, timer.getId());
	}

	private List<Field> gatherSnapshotFields(HistogramSnapshot snapshot) {
		List<Field> fields = new ArrayList<>();
		fields.add(new Field("sum", snapshot.total(getBaseTimeUnit())));
		fields.add(new Field("count", snapshot.count()));
		fields.add(new Field("mean", snapshot.mean(getBaseTimeUnit())));
		fields.add(new Field("upper", snapshot.max(getBaseTimeUnit())));
		fields.add(new Field("total", snapshot.total(getBaseTimeUnit())));

		for (ValueAtPercentile v : snapshot.percentileValues()) {
			fields.add(new Field(format.format(v.percentile()) + "_percentile", v.value(getBaseTimeUnit())));
		}
		return fields;
	}

	private Map<String, Object> toMeterMap(List<Field> fields, Meter.Id id) {
		Map<String, Object> meterMap = new LinkedHashMap<>();
		meterMap.put("id", id);
		meterMap.put("metrics", fields);
		return meterMap;
	}

	/**
	 *
	 */
	private class Field {
		final String name;
		final double value;

		private Field(String name, double value) {
			this.name = name;
			this.value = value;
		}

		@SuppressWarnings("unused") // used by ObjectMapper
		public String getName() {
			return name;
		}

		@SuppressWarnings("unused") // used by ObjectMapper
		public double getValue() {
			return value;
		}

		@Override
		public String toString() {
			return name + "=" + format.format(value);
		}
	}

	/**
	 *
	 */
	private static final class MessageChannelPublisher implements Consumer<String> {
		private final MetersPublisherBinding metersPublisherBinding;

		MessageChannelPublisher(MetersPublisherBinding metersPublisherBinding) {
			this.metersPublisherBinding = metersPublisherBinding;
		}

		@Override
		public void accept(String metricData) {
			logger.trace(metricData);
			this.metersPublisherBinding.applicationMetrics().send(new GenericMessage<String>(metricData));
		}
	}
}
