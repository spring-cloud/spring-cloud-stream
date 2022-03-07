/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.micrometer;

import java.util.ArrayList;
import java.util.List;
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
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultMeter;
import io.micrometer.core.instrument.step.StepCounter;
import io.micrometer.core.instrument.step.StepDistributionSummary;
import io.micrometer.core.instrument.step.StepFunctionCounter;
import io.micrometer.core.instrument.step.StepFunctionTimer;
import io.micrometer.core.instrument.step.StepTimer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Oleg Zhurakousky
 * @since 2.0
 */
class DefaultDestinationPublishingMeterRegistry extends MeterRegistry
		implements SmartLifecycle {

	private static final Log logger = LogFactory
			.getLog(DefaultDestinationPublishingMeterRegistry.class);

	private final MetricsPublisherConfig metricsPublisherConfig;

	private final Consumer<String> metricsConsumer;

	private final ApplicationMetricsProperties applicationProperties;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private ScheduledFuture<?> publisher;

	DefaultDestinationPublishingMeterRegistry(
			ApplicationMetricsProperties applicationProperties,
			MetersPublisherBinding publisherBinding,
			MetricsPublisherConfig metricsPublisherConfig, Clock clock) {
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
		if (this.publisher != null) {
			this.publisher.cancel(false);
			this.publisher = null;
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
		return new StepCounter(id, this.clock,
				this.metricsPublisherConfig.step().toMillis());
	}

//	@Override
//	protected LongTaskTimer newLongTaskTimer(Meter.Id id) {
//		return new DefaultLongTaskTimer(id, this.clock);
//	}

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}

	protected void publish() {
		List<Metric<Number>> aggregatedMeters = new ArrayList<>();
		for (Meter meter : this.getMeters()) {
			if (meter instanceof Timer) {
				aggregatedMeters.add(toTimerMetric((Timer) meter));
			}
			else if (meter instanceof DistributionSummary) {
				aggregatedMeters.add(toSummaryMetric((DistributionSummary) meter));
			}
		}
		if (!aggregatedMeters.isEmpty()) {
			ApplicationMetrics metrics = new ApplicationMetrics(
					this.applicationProperties.getKey(), aggregatedMeters);
			metrics.setInterval(this.metricsPublisherConfig.step().toMillis());
			metrics.setProperties(this.applicationProperties.getExportProperties());
			try {
				String jsonString = this.objectMapper.writeValueAsString(metrics);
				this.metricsConsumer.accept(jsonString);
			}
			catch (JsonProcessingException e) {
				logger.warn("Error producing JSON String representation metric data", e);
			}
		}
	}

	@Override
	protected Timer newTimer(Id id,
			DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		return new StepTimer(id, this.clock, distributionStatisticConfig, pauseDetector,
				getBaseTimeUnit(), this.metricsPublisherConfig.step().toMillis(), false);
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Id id, T obj,
			ToLongFunction<T> countFunction, ToDoubleFunction<T> totalTimeFunction,
			TimeUnit totalTimeFunctionUnits) {
		return new StepFunctionTimer<T>(id, this.clock,
				this.metricsPublisherConfig.step().toMillis(), obj, countFunction,
				totalTimeFunction, totalTimeFunctionUnits, getBaseTimeUnit());
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Id id, T obj,
			ToDoubleFunction<T> valueFunction) {
		return new StepFunctionCounter<T>(id, this.clock,
				this.metricsPublisherConfig.step().toMillis(), obj, valueFunction);
	}

	@Override
	protected Meter newMeter(Id id, Type type, Iterable<Measurement> measurements) {
		return new DefaultMeter(id, type, measurements);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Id id,
			DistributionStatisticConfig distributionStatisticConfig, double scale) {
		return new StepDistributionSummary(id, this.clock, distributionStatisticConfig,
				scale, this.metricsPublisherConfig.step().toMillis(), false);
	}

	@Override
	protected DistributionStatisticConfig defaultHistogramConfig() {
		return DistributionStatisticConfig.builder()
				.expiry(this.metricsPublisherConfig.step()).build()
				.merge(DistributionStatisticConfig.DEFAULT);
	}

	private void start(ThreadFactory threadFactory) {
		if (this.publisher != null) {
			stop();
		}
		this.publisher = Executors.newSingleThreadScheduledExecutor(threadFactory)
				.scheduleAtFixedRate(this::publish,
						this.metricsPublisherConfig.step().toMillis(),
						this.metricsPublisherConfig.step().toMillis(),
						TimeUnit.MILLISECONDS);
	}

	private Metric<Number> toSummaryMetric(DistributionSummary summary) {
		return new Metric<Number>(summary.getId(), summary.takeSnapshot());
	}

	private Metric<Number> toTimerMetric(Timer timer) {
		return new Metric<Number>(timer.getId(), timer.takeSnapshot());
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		// TODO Auto-generated method stub
		return null;
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
			Message<String> message = MessageBuilder.withPayload(metricData)
					.setHeader("STREAM_CLOUD_STREAM_VERSION", "2.x").build();
			this.metersPublisherBinding.applicationMetrics().send(message);
		}

	}
}
