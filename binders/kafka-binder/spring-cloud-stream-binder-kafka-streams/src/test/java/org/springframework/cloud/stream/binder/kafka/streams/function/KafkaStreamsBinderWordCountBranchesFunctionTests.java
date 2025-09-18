/*
 * Copyright 2019-present the original author or authors.
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
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Soby Chacko
 */
@EmbeddedKafka(topics = {"counts", "foo", "bar"})
class KafkaStreamsBinderWordCountBranchesFunctionTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	private static Consumer<String, String> consumer;

	@BeforeAll
	public static void setUp() throws Exception {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "groupx", false);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "counts", "foo", "bar");
	}

	@AfterAll
	public static void tearDown() {
		consumer.close();
	}

	@Test
	@Disabled
	void kstreamWordCountWithStringInputAndPojoOuput(EmbeddedKafkaBroker embeddedKafka) throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		ConfigurableApplicationContext context = app.run("--server.port=0",
				"--spring.cloud.stream.function.bindings.process-in-0=input",
				"--spring.cloud.stream.bindings.input.destination=words",
				"--spring.cloud.stream.function.bindings.process-out-0=output1",
				"--spring.cloud.stream.bindings.output1.destination=counts",
				"--spring.cloud.stream.function.bindings.process-out-1=output2",
				"--spring.cloud.stream.bindings.output2.destination=foo",
				"--spring.cloud.stream.function.bindings.process-out-2=output3",
				"--spring.cloud.stream.bindings.output3.destination=bar",

				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.applicationId" +
						"=KafkaStreamsBinderWordCountBranchesFunctionTests-abc",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString());
		try {
			receiveAndValidate(embeddedKafka);
		}
		finally {
			context.close();
		}
	}

	private void receiveAndValidate(EmbeddedKafkaBroker embeddedKafka) throws Exception {
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

		@Bean
		@SuppressWarnings({"unchecked"})
		public Function<KStream<Object, String>, KStream<?, WordCount>[]> process() {

			Predicate<Object, WordCount> isEnglish = (k, v) -> v.word.equals("english");
			Predicate<Object, WordCount> isFrench = (k, v) -> v.word.equals("french");
			Predicate<Object, WordCount> isSpanish = (k, v) -> v.word.equals("spanish");

			return input -> {
				final Map<String, KStream<Object, WordCount>> stringKStreamMap = input
						.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.ROOT).split("\\W+")))
						.groupBy((key, value) -> value)
						.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
						.count(Materialized.as("WordCounts-branch"))
						.toStream()
						.map((key, value) -> new KeyValue<>(null, new WordCount(key.key(), value,
								new Date(key.window().start()), new Date(key.window().end()))))
						.split()
						.branch(isEnglish)
						.branch(isFrench)
						.branch(isSpanish)
						.noDefaultBranch();

				return stringKStreamMap.values().toArray(new KStream[0]);
			};
		}
	}

}
