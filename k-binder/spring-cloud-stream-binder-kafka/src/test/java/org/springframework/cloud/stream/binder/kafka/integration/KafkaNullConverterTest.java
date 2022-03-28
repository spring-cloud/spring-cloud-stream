/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.integration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Aldo Sinanaj
 * @author Gary Russell
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
		"spring.kafka.consumer.auto-offset-reset=earliest" })
@DirtiesContext
@Ignore
public class KafkaNullConverterTest {

	private static final String KAFKA_BROKERS_PROPERTY = "spring.kafka.bootstrap-servers";

	@Autowired
	private MessageChannel kafkaNullOutput;

	@Autowired
	private MessageChannel kafkaNullInput;

	@Autowired
	private KafkaNullConverterTestConfig config;

	@ClassRule
	public static EmbeddedKafkaRule kafkaEmbedded = new EmbeddedKafkaRule(1, true);

	@BeforeClass
	public static void setup() {
		System.setProperty(KAFKA_BROKERS_PROPERTY,
				kafkaEmbedded.getEmbeddedKafka().getBrokersAsString());
	}

	@AfterClass
	public static void clean() {
		System.clearProperty(KAFKA_BROKERS_PROPERTY);
	}

	@Test
	public void testKafkaNullConverterOutput() throws InterruptedException {
		this.kafkaNullOutput.send(new GenericMessage<>(KafkaNull.INSTANCE));

		assertThat(this.config.countDownLatchOutput.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.outputPayload).isNull();
	}

	@Test
	public void testKafkaNullConverterInput() throws InterruptedException {
		this.kafkaNullInput.send(new GenericMessage<>(KafkaNull.INSTANCE));

		assertThat(this.config.countDownLatchInput.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.inputPayload).isNull();
	}

	@TestConfiguration
	@EnableBinding(KafkaNullTestChannels.class)
	public static class KafkaNullConverterTestConfig {

		final CountDownLatch countDownLatchOutput = new CountDownLatch(1);

		final CountDownLatch countDownLatchInput = new CountDownLatch(1);

		volatile byte[] outputPayload = new byte[0];

		volatile byte[] inputPayload = new byte[0];

		@KafkaListener(id = "foo", topics = "kafkaNullOutput")
		public void listen(@Payload(required = false) byte[] in) {
			this.outputPayload = in;
			countDownLatchOutput.countDown();
		}

		@StreamListener("kafkaNullInput")
		public void inputListen(@Payload(required = false) byte[] payload) {
			this.inputPayload = payload;
			countDownLatchInput.countDown();
		}

	}

	public interface KafkaNullTestChannels {

		@Input
		MessageChannel kafkaNullInput();

		@Output
		MessageChannel kafkaNullOutput();

	}

}
