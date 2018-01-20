/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kstream.config.KStreamApplicationSupportProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Gary Russell
 */
public class WordCountMultipleBranchesIntegrationTests {

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "counts","foo","bar");

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setUp() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("groupx", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "counts", "foo", "bar");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testKstreamWordCountWithStringInputAndPojoOuput() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebEnvironment(false);

		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.input.destination=words",
				"--spring.cloud.stream.bindings.output1.destination=counts",
				"--spring.cloud.stream.bindings.output1.contentType=application/json",
				"--spring.cloud.stream.bindings.output2.destination=foo",
				"--spring.cloud.stream.bindings.output2.contentType=application/json",
				"--spring.cloud.stream.bindings.output3.destination=bar",
				"--spring.cloud.stream.bindings.output3.contentType=application/json",
				"--spring.cloud.stream.kstream.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kstream.binder.configuration.key.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kstream.binder.configuration.value.serde=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.bindings.output.producer.headerMode=raw",
				"--spring.cloud.stream.bindings.input.consumer.headerMode=raw",
				"--spring.cloud.stream.kstream.timeWindow.length=5000",
				"--spring.cloud.stream.kstream.timeWindow.advanceBy=0",
				"--spring.cloud.stream.kstream.binder.brokers=" + embeddedKafka.getBrokersAsString(),
				"--spring.cloud.stream.kstream.binder.zkNodes=" + embeddedKafka.getZookeeperConnectionString());
		try {
			receiveAndValidate(context);
		} finally {
			context.close();
		}
	}

	private void receiveAndValidate(ConfigurableApplicationContext context) throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
		template.setDefaultTopic("words");
		template.sendDefault("english");
		ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, "counts");
		assertThat(cr.value().contains("\"word\":\"english\",\"count\":1")).isTrue();

		template.sendDefault("french");
		template.sendDefault("french");
		cr = KafkaTestUtils.getSingleRecord(consumer, "foo");
		assertThat(cr.value().contains("\"word\":\"french\",\"count\":2")).isTrue();

		template.sendDefault("spanish");
		template.sendDefault("spanish");
		template.sendDefault("spanish");
		cr = KafkaTestUtils.getSingleRecord(consumer, "bar");
		assertThat(cr.value().contains("\"word\":\"spanish\",\"count\":3")).isTrue();
	}

	@EnableBinding(KStreamProcessorX.class)
	@EnableAutoConfiguration
	@EnableConfigurationProperties(KStreamApplicationSupportProperties.class)
	public static class WordCountProcessorApplication {

		@Autowired
		private TimeWindows timeWindows;

		@StreamListener("input")
		@SendTo({"output1","output2","output3"})
		@SuppressWarnings("unchecked")
		public KStream<?, WordCount>[] process(KStream<Object, String> input) {

			Predicate<Object, WordCount> isEnglish = (k, v) -> v.word.equals("english");
			Predicate<Object, WordCount> isFrench =  (k, v) -> v.word.equals("french");
			Predicate<Object, WordCount> isSpanish = (k, v) -> v.word.equals("spanish");

			return input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.groupBy((key, value) -> value)
					.windowedBy(timeWindows)
					.count(Materialized.as("WordCounts-multi"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
					.branch(isEnglish, isFrench, isSpanish);
		}
	}

	interface KStreamProcessorX {

		@Input("input")
		KStream<?, ?> input();

		@Output("output1")
		KStream<?, ?> output1();

		@Output("output2")
		KStream<?, ?> output2();

		@Output("output3")
		KStream<?, ?> output3();
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

}
