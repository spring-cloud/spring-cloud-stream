/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

import org.springframework.cloud.stream.binder.kafka.common.TopicInformation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that offset-lag computations performed by {@link KafkaBinderMetrics} do
 * not trigger kafka-clients' 'Not updating ... as it is no longer assigned' WARN
 * logging (see GH-3208, KAFKA-20131, KAFKA-20449).
 */
@EmbeddedKafka(topics = KafkaBinderMetricsLoggingTests.TEST_TOPIC)
class KafkaBinderMetricsLoggingTests {

	static final String TEST_TOPIC = "metrics-logging-topic";

	private static final String OFFSET_FETCHER_UTILS_LOGGER =
			"org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils";

	private static EmbeddedKafkaBroker embeddedKafka;

	private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

	private Logger offsetFetcherUtilsLogger;

	private Level originalLevel;

	private ListAppender<ILoggingEvent> logAppender;

	@BeforeAll
	static void startBroker() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@AfterEach
	void restoreLogging() {
		if (this.offsetFetcherUtilsLogger != null) {
			this.offsetFetcherUtilsLogger.detachAppender(this.logAppender);
			this.offsetFetcherUtilsLogger.setLevel(this.originalLevel);
		}
	}

	@Test
	void repeatedOffsetLagComputationsProduceNoNoLongerAssignedWarnings() {
		captureKafkaClientWarnings();

		KafkaBinderMetrics metrics = new KafkaBinderMetrics(binderWithTopicInUse(),
				binderConfigurationProperties());
		metrics.bindTo(this.meterRegistry);

		// Two consecutive synchronous computations - each one calls endOffsets()
		// on the metadata consumer and would emit a WARN per partition on
		// affected kafka-clients versions (4.0.x - 4.3.x) without the fix.
		this.meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
			.tag("group", "metricsLoggingGroup").tag("topic", TEST_TOPIC).gauge().value();
		this.meterRegistry.get(KafkaBinderMetrics.OFFSET_LAG_METRIC_NAME)
			.tag("group", "metricsLoggingGroup").tag("topic", TEST_TOPIC).gauge().value();

		List<String> warnings = this.logAppender.list.stream()
			.filter(event -> event.getLevel() == Level.WARN)
			.map(ILoggingEvent::getFormattedMessage)
			.filter(message -> message.contains("no longer assigned"))
			.collect(Collectors.toList());
		assertThat(warnings)
			.as("kafka-clients must not log 'no longer assigned' warnings for "
					+ "binder offset-lag computations, but found: %s", warnings)
			.isEmpty();
	}

	private KafkaMessageChannelBinder binderWithTopicInUse() {
		Map<String, TopicInformation> topicsInUse = new HashMap<>();
		topicsInUse.put(TEST_TOPIC,
				new TopicInformation("metricsLoggingGroup", null, true));
		KafkaMessageChannelBinder binder = mock(KafkaMessageChannelBinder.class);
		when(binder.getTopicsInUse()).thenReturn(topicsInUse);
		return binder;
	}

	@SuppressWarnings("unchecked")
	private KafkaBinderConfigurationProperties binderConfigurationProperties() {
		KafkaProperties kafkaProperties = new KafkaProperties();
		KafkaBinderConfigurationProperties properties =
				new KafkaBinderConfigurationProperties(kafkaProperties,
						mock(ObjectProvider.class));
		properties.setBrokers(embeddedKafka.getBrokersAsString());
		// Mirrors the reporting application: read_committed selects the
		// 'last stable offset' variant of the kafka-clients warning.
		properties.getConfiguration().put("isolation.level", "read_committed");
		return properties;
	}

	private void captureKafkaClientWarnings() {
		this.offsetFetcherUtilsLogger = (Logger) LoggerFactory
			.getLogger(OFFSET_FETCHER_UTILS_LOGGER);
		this.originalLevel = this.offsetFetcherUtilsLogger.getLevel();
		this.offsetFetcherUtilsLogger.setLevel(Level.DEBUG);
		this.logAppender = new ListAppender<>();
		this.logAppender.start();
		this.offsetFetcherUtilsLogger.addAppender(this.logAppender);
	}

}
