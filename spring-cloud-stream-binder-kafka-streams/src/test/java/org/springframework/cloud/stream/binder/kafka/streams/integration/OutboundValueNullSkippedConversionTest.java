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

package org.springframework.cloud.stream.binder.kafka.streams.integration;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
public class OutboundValueNullSkippedConversionTest {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"counts");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule
			.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
				consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "counts");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	// The following test verifies the fixes made for this issue:
	// https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/774
	@Test
	public void testOutboundNullValueIsHandledGracefully()
			throws Exception {
		SpringApplication app = new SpringApplication(
				OutboundNullApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=words",
				"--spring.cloud.stream.bindings.output.destination=counts",
				"--spring.cloud.stream.bindings.output.producer.useNativeEncoding=false",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testOutboundNullValueIsHandledGracefully",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.timeWindow.length=5000",
				"--spring.cloud.stream.kafka.streams.timeWindow.advanceBy=0",
				"--spring.cloud.stream.kafka.binder.brokers="
						+ embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("words");
				template.sendDefault("foobar");
				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer,
						"counts");
				assertThat(cr.value() == null).isTrue();
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableBinding(KafkaStreamsProcessor.class)
	@EnableAutoConfiguration
	static class OutboundNullApplication {

		@StreamListener
		@SendTo("output")
		public KStream<?, KafkaStreamsBinderWordCountIntegrationTests.WordCount> process(
				@Input("input") KStream<Object, String> input) {

			return input
					.flatMapValues(
							value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofSeconds(5))).count(Materialized.as("foo-WordCounts"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null, null));
		}
	}
}
