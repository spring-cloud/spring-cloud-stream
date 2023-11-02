/*
 * Copyright 2019-2023 the original author or authors.
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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsRegistry;
import org.springframework.cloud.stream.binder.kafka.streams.StreamsBuilderFactoryManager;
import org.springframework.cloud.stream.binder.kafka.streams.endpoint.KafkaStreamsTopologyEndpoint;
import org.springframework.cloud.stream.binding.InputBindingLifecycle;
import org.springframework.cloud.stream.binding.OutputBindingLifecycle;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = {"counts", "counts-1", "counts-2", "counts-5", "counts-6"})
class KafkaStreamsBinderWordCountFunctionTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	private static Consumer<String, String> consumer;

	private final static CountDownLatch LATCH = new CountDownLatch(1);

	@BeforeAll
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "counts", "counts-1", "counts-2", "counts-5",  "counts-6");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testBasicKStreamTopologyExecution() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts",
				"--spring.cloud.stream.kafka.streams.binder.application-id=testKstreamWordCountFunction",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.consumerProperties.request.timeout.ms=29000", //for testing ...binder.consumerProperties
				"--spring.cloud.stream.kafka.streams.binder.consumerProperties.consumer.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
				"--spring.cloud.stream.kafka.streams.binder.producerProperties.max.block.ms=90000", //for testing ...binder.producerProperties
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.consumedAs=custom-consumer",
				"--spring.cloud.stream.kafka.streams.bindings.process-out-0.producer.producedAs=custom-producer",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("words", "counts");

			final MeterRegistry meterRegistry = context.getBean(MeterRegistry.class);
			Thread.sleep(100);

			assertThat(meterRegistry.getMeters().stream().anyMatch(m -> m.getId().getName().equals("kafka.stream.thread.poll.records.max"))).isTrue();
			assertThat(meterRegistry.getMeters().stream().anyMatch(m -> m.getId().getName().equals("kafka.consumer.network.io.total"))).isTrue();
			assertThat(meterRegistry.getMeters().stream().anyMatch(m -> m.getId().getName().equals("kafka.producer.record.send.total"))).isTrue();
			assertThat(meterRegistry.getMeters().stream().anyMatch(m -> m.getId().getName().equals("kafka.admin.client.network.io.total"))).isTrue();

			Assert.isTrue(LATCH.await(5, TimeUnit.SECONDS), "Failed to call customizers");
			//Testing topology endpoint
			final KafkaStreamsRegistry kafkaStreamsRegistry = context.getBean(KafkaStreamsRegistry.class);
			final KafkaStreamsTopologyEndpoint kafkaStreamsTopologyEndpoint = new KafkaStreamsTopologyEndpoint(kafkaStreamsRegistry);
			final List<String> topologies = kafkaStreamsTopologyEndpoint.kafkaStreamsTopologies();
			final String topology1 = topologies.get(0);
			final String topology2 = kafkaStreamsTopologyEndpoint.kafkaStreamsTopology("testKstreamWordCountFunction");
			assertThat(topology1).isNotEmpty();
			assertThat(topology1).isEqualTo(topology2);
			assertThat(topology1.contains("Source: custom-consumer")).isTrue();
			assertThat(topology1.contains("Sink: custom-producer")).isTrue();

			//verify that ...binder.consumerProperties and ...binder.producerProperties work.
			Map<String, Object> streamConfigGlobalProperties = (Map<String, Object>) context.getBean("streamConfigGlobalProperties");
			assertThat(streamConfigGlobalProperties.get("consumer.request.timeout.ms")).isEqualTo("29000");
			assertThat(streamConfigGlobalProperties.get("consumer.value.deserializer")).isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");
			assertThat(streamConfigGlobalProperties.get("producer.max.block.ms")).isEqualTo("90000");

			InputBindingLifecycle inputBindingLifecycle = context.getBean(InputBindingLifecycle.class);
			final Collection<Binding<Object>> inputBindings = (Collection<Binding<Object>>) new DirectFieldAccessor(inputBindingLifecycle)
					.getPropertyValue("inputBindings");
			assertThat(inputBindings).isNotNull();
			final Optional<Binding<Object>> theOnlyInputBinding = inputBindings.stream().findFirst();
			assertThat(theOnlyInputBinding.isPresent()).isTrue();
			final DefaultBinding<Object> objectBinding = (DefaultBinding<Object>) theOnlyInputBinding.get();
			assertThat(objectBinding.getBindingName()).isEqualTo("process-in-0");

			final Lifecycle lifecycle = (Lifecycle) new DirectFieldAccessor(objectBinding).getPropertyValue("lifecycle");
			final StreamsBuilderFactoryBean streamsBuilderFactoryBean = context.getBean(StreamsBuilderFactoryBean.class);
			assertThat(lifecycle).isEqualTo(streamsBuilderFactoryBean);

			OutputBindingLifecycle outputBindingLifecycle = context.getBean(OutputBindingLifecycle.class);
			final Collection<Binding<Object>> outputBindings = (Collection<Binding<Object>>) new DirectFieldAccessor(outputBindingLifecycle)
					.getPropertyValue("outputBindings");
			assertThat(outputBindings).isNotNull();
			final Optional<Binding<Object>> theOnlyOutputBinding = outputBindings.stream().findFirst();
			assertThat(theOnlyOutputBinding.isPresent()).isTrue();
			final DefaultBinding<Object> objectBinding1 = (DefaultBinding<Object>) theOnlyOutputBinding.get();
			assertThat(objectBinding1.getBindingName()).isEqualTo("process-out-0");

			final Lifecycle lifecycle1 = (Lifecycle) new DirectFieldAccessor(objectBinding1).getPropertyValue("lifecycle");
			assertThat(lifecycle1).isEqualTo(streamsBuilderFactoryBean);
		}
	}

	@Test
	void testKstreamWordCountWithApplicationIdSpecifiedAtDefaultConsumer() {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-5",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-5",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testKstreamWordCountWithApplicationIdSpecifiedAtDefaultConsumer",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.binder.brokers="
						+ embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("words-5", "counts-5");
		}
	}

	@Test
	void testKstreamWordCountFunctionWithCustomProducerStreamPartitioner() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.streams.binder.application-id=testKstreamWordCountFunctionWithCustomProducerStreamPartitioner",
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

	@Test
	void testKstreamBinderAutoStartup() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.kafka.streams.auto-startup=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-3",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-3",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			final StreamsBuilderFactoryManager streamsBuilderFactoryManager = context.getBean(StreamsBuilderFactoryManager.class);
			assertThat(streamsBuilderFactoryManager.isAutoStartup()).isFalse();
			assertThat(streamsBuilderFactoryManager.isRunning()).isFalse();
		}
	}

	@Test
	void testKstreamIndividualBindingAutoStartup() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-4",
				"--spring.cloud.stream.bindings.process-in-0.consumer.auto-startup=false",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-4",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			final StreamsBuilderFactoryBean streamsBuilderFactoryBean = context.getBean(StreamsBuilderFactoryBean.class);
			assertThat(streamsBuilderFactoryBean.isRunning()).isFalse();
			streamsBuilderFactoryBean.start();
			assertThat(streamsBuilderFactoryBean.isRunning()).isTrue();
		}
	}

	// The following test verifies the fixes made for this issue:
	// https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/774
	@Test
	void testOutboundNullValueIsHandledGracefully()
			throws Exception {
		SpringApplication app = new SpringApplication(OutboundNullApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process-in-0.destination=words-6",
				"--spring.cloud.stream.bindings.process-out-0.destination=counts-6",
				"--spring.cloud.stream.bindings.process-out-0.producer.useNativeEncoding=false",
				"--spring.cloud.stream.kafka.streams.default.consumer.application-id=testOutboundNullValueIsHandledGracefully",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde"
						+ "=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.binder.brokers="
						+ embeddedKafka.getBrokersAsString())) {

			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("words-6");
				template.sendDefault("foobar");
				ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer,
						"counts-6");
				assertThat(cr.value() == null).isTrue();
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
		Function<KStream<Object, String>, KStream<String, WordCount>> process() {

			return input -> input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
					.count(Materialized.as("foo-WordCounts"))
					.toStream()
					.map((key, value) -> new KeyValue<>(key.key(), new WordCount(key.key(), value,
							new Date(key.window().start()), new Date(key.window().end()))));
		}

		@Bean
		StreamsBuilderFactoryBeanConfigurer customizer() {
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
		StreamPartitioner<String, WordCount> streamPartitioner() {
			return (t, k, v, n) -> k.equals("foo") ? 0 : 1;
		}
	}

	@EnableAutoConfiguration
	static class OutboundNullApplication {

		@Bean
		Function<KStream<Object, String>, KStream<?, WordCount>> process() {
			return input -> input
					.flatMapValues(
							value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofSeconds(5))).count(Materialized.as("foobar-WordCounts"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null, null));
		}
	}
}
