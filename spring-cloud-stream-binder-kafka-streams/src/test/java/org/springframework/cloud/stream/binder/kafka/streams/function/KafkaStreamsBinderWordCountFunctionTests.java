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

package org.springframework.cloud.stream.binder.kafka.streams.function;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsRegistry;
import org.springframework.cloud.stream.binder.kafka.streams.endpoint.KafkaStreamsTopologyEndpoint;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaStreamsBinderWordCountFunctionTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"counts", "counts-1", "counts-2");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	private final static CountDownLatch LATCH = new CountDownLatch(1);

	@BeforeClass
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "counts", "counts-1", "counts-2");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testKstreamWordCountFunction() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testKstreamWordCountFunction",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("words", "counts");
			final MeterRegistry meterRegistry = context.getBean(MeterRegistry.class);
			Thread.sleep(100);
			assertThat(meterRegistry.getMeters().size() > 1).isTrue();
			Assert.isTrue(LATCH.await(5, TimeUnit.SECONDS), "Failed to call customizers");
			//Testing topology endpoint
			final KafkaStreamsRegistry kafkaStreamsRegistry = context.getBean(KafkaStreamsRegistry.class);
			final KafkaStreamsTopologyEndpoint kafkaStreamsTopologyEndpoint = new KafkaStreamsTopologyEndpoint(kafkaStreamsRegistry);
			final String topology1 = kafkaStreamsTopologyEndpoint.kafkaStreamsTopology();
			final String topology2 = kafkaStreamsTopologyEndpoint.kafkaStreamsTopology("testKstreamWordCountFunction");
			assertThat(topology1).isNotEmpty();
			assertThat(topology1).isEqualTo(topology2);
		}
	}

	@Test
	public void testKstreamWordCountFunctionWithGeneratedApplicationId() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-1",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-1",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("words-1", "counts-1");
		}
	}

	@Test
	public void testKstreamWordCountFunctionWithCustomProducerStreamPartitioner() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-2",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-2",
				"--spring.cloud.stream.bindings.process-out-0.producer.partitionCount=2",
				"--spring.cloud.stream.kafka.streams.bindings.process-out-0.producer.streamPartitionerBeanName" +
						"=streamPartitioner",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("words-2");
				template.sendDefault("foo");
				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, "counts-2");
				assertThat(cr.value().contains("\"word\":\"foo\",\"count\":1")).isTrue();
				assertThat(cr.partition() == 0) .isTrue();
				template.sendDefault("bar");
				cr = KafkaTestUtils.getSingleRecord(consumer, "counts-2");
				assertThat(cr.value().contains("\"word\":\"bar\",\"count\":1")).isTrue();
				assertThat(cr.partition() == 1) .isTrue();
			}
			finally {
				pf.destroy();
			}
		}
	}

	private void receiveAndValidate(String in, String out) {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic(in);
			template.sendDefault("foobar");
			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, out);
			assertThat(cr.value().contains("\"word\":\"foobar\",\"count\":1")).isTrue();
		}
		finally {
			pf.destroy();
		}
	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}

	@EnableAutoConfiguration
	public static class WordCountProcessorApplication {

		@Autowired
		InteractiveQueryService interactiveQueryService;

		@Bean
		public Function<KStream<Object, String>, KStream<String, WordCount>> process() {

			return input -> input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(5000))
					.count(Materialized.as("foo-WordCounts"))
					.toStream()
					.map((key, value) -> new KeyValue<>(key.key(), new WordCount(key.key(), value,
							new Date(key.window().start()), new Date(key.window().end()))));
		}

		@Bean
		public StreamsBuilderFactoryBeanCustomizer customizer() {
			return fb -> {
				try {
					fb.setStateListener((newState, oldState) -> {

					});
					fb.getObject(); //make sure no exception is thrown at this call.
					KafkaStreamsBinderWordCountFunctionTests.LATCH.countDown();

				}
				catch (Exception e) {
					//Nothing to do - When the exception is thrown above, the latch won't be count down.
				}
			};
		}

		@Bean
		public StreamPartitioner<String, WordCount> streamPartitioner() {
			return (t, k, v, n) -> k.equals("foo") ? 0 : 1;
		}
	}
}
