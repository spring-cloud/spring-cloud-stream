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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.Assert;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipleFunctionsInSameAppTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true,
			"coffee", "electronics");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<String, String> consumer;

	private static CountDownLatch countDownLatch = new CountDownLatch(2);

	@BeforeClass
	public static void setUp() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("purchase-groups", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromEmbeddedTopics(consumer, "coffee", "electronics");
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testKstreamWordCountFunction() throws InterruptedException {
		SpringApplication app = new SpringApplication(MultipleFunctionsInSameApp.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		try (ConfigurableApplicationContext ignored = app.run(
				"--server.port=0",
				"--spring.jmx.enabled=false",
				"--spring.cloud.stream.bindings.process_in.destination=purchases",
				"--spring.cloud.stream.bindings.process_out_0.destination=coffee",
				"--spring.cloud.stream.bindings.process_out_1.destination=electronics",
				"--spring.cloud.stream.bindings.analyze_in_0.destination=coffee",
				"--spring.cloud.stream.bindings.analyze_in_1.destination=electronics",
				"--spring.cloud.stream.kafka.streams.binder.functions.analyze.applicationId=analyze-id-0",
				"--spring.cloud.stream.kafka.streams.binder.functions.process.applicationId=process-id-0",
				"--spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.key.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.configuration.default.value.serde" +
						"=org.apache.kafka.common.serialization.Serdes$StringSerde",
				"--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString())) {
			receiveAndValidate("purchases", "coffee", "electronics");
		}
	}

	private void receiveAndValidate(String in, String... out) throws InterruptedException {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		try {
			KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic(in);
			template.sendDefault("coffee");
			ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, out[0]);
			assertThat(cr.value().contains("coffee")).isTrue();

			template.sendDefault("electronics");
			cr = KafkaTestUtils.getSingleRecord(consumer, out[1]);
			assertThat(cr.value().contains("electronics")).isTrue();

			Assert.isTrue(countDownLatch.await(5, TimeUnit.SECONDS), "Analyze (BiConsumer) method didn't receive all the expected records");
		}
		finally {
			pf.destroy();
		}
	}

	@EnableAutoConfiguration
	public static class MultipleFunctionsInSameApp {

		@Bean
		public Function<KStream<String, String>, KStream<String, String>[]> process() {
			return input -> input.branch(
					(s, p) -> p.equalsIgnoreCase("coffee"),
					(s, p) -> p.equalsIgnoreCase("electronics"));
		}

		@Bean
		public BiConsumer<KStream<String, String>, KStream<String, String>> analyze() {
			return (coffee, electronics) -> {
				coffee.foreach((s, p) -> countDownLatch.countDown());
				electronics.foreach((s, p) -> countDownLatch.countDown());
			};
		}
	}
}
