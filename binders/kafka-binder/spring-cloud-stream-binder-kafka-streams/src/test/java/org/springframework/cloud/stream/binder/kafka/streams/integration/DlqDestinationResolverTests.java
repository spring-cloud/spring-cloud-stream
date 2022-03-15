/*
 * Copyright 2020-2022 the original author or authors.
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
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsBindingInformationCatalogue;
import org.springframework.cloud.stream.binder.kafka.utils.DlqDestinationResolver;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
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
@EmbeddedKafka(topics = {"topic1-dlq", "topic2-dlq"})
public class DlqDestinationResolverTests {

	private static final EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();

	@Test
	void testDlqDestinationResolverWorks() throws Exception {
		SpringApplication app = new SpringApplication(WordCountProcessorApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext context = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.function.definition=process",
				"--spring.cloud.stream.bindings.process-in-0.destination=word1,word2",
				"--spring.cloud.stream.bindings.process-out-0.destination=test-output",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.application-id=dlq-dest-resolver-test",
				"--spring.cloud.stream.kafka.streams.binder.serdeError=sendToDlq",
				"--spring.cloud.stream.kafka.streams.bindings.process-in-0.consumer.valueSerde="
						+ "org.apache.kafka.common.serialization.Serdes$IntegerSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(
					senderProps);
			try {
				KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
				template.setDefaultTopic("word1");
				template.sendDefault("foobar");

				template.setDefaultTopic("word2");
				template.sendDefault("foobar");

				Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("some-random-group",
						"false", embeddedKafka);
				consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
				DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
						consumerProps);
				Consumer<String, String> consumer1 = cf.createConsumer();
				embeddedKafka.consumeFromEmbeddedTopics(consumer1, "topic1-dlq",
						"topic2-dlq");

				ConsumerRecord<String, String> cr1 = KafkaTestUtils.getSingleRecord(consumer1,
						"topic1-dlq");
				assertThat(cr1.value()).isEqualTo("foobar");
				ConsumerRecord<String, String> cr2 = KafkaTestUtils.getSingleRecord(consumer1,
						"topic2-dlq");
				assertThat(cr2.value()).isEqualTo("foobar");

				final KafkaStreamsBindingInformationCatalogue catalogue = context.getBean(KafkaStreamsBindingInformationCatalogue.class);
				assertThat(catalogue.getDlqProducerFactories().size()).isEqualTo(1);
			}
			finally {
				pf.destroy();
			}
		}
	}

	@EnableAutoConfiguration
	public static class WordCountProcessorApplication {

		@Bean
		public Function<KStream<Object, String>, KStream<?, String>> process() {

			return input -> input
					.flatMapValues(
							value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.windowedBy(TimeWindows.of(Duration.ofSeconds(5))).count(Materialized.as("foo-WordCounts-x"))
					.toStream().map((key, value) -> new KeyValue<>(null,
							"Count for " + key.key() + " : " + value));
		}

		@Bean
		public DlqPartitionFunction partitionFunction() {
			return (group, rec, ex) -> 0;
		}

		@Bean
		public DlqDestinationResolver dlqDestinationResolver() {
			return (rec, ex) -> {
				if (rec.topic().equals("word1")) {
					return "topic1-dlq";
				}
				return "topic2-dlq";
			};
		}
	}
}
