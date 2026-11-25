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

import java.lang.reflect.Proxy;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.SendResultListener;
import org.springframework.cloud.stream.binder.kafka.config.ClientFactoryCustomizer;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for notifying a {@link SendResultListener} of the outcome of sending with
 * {@link StreamBridge} through the Kafka binder.
 *
 * @author Oleg Zhurakousky
 */
@EmbeddedKafka
@DirtiesContext
class StreamBridgeSendResultListenerTests {

	@Test
	void listenerIsNotifiedOfSuccessfulSend() throws Exception {
		try (ConfigurableApplicationContext context = run("success")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			RecordingListener listener = context.getBean(RecordingListener.class);

			streamBridge.send("success-out", MessageBuilder.withPayload("hello").setHeader("correlation", "abc").build());

			assertThat(listener.successLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(listener.bindingName).isEqualTo("success-out");
			assertThat(listener.message.getHeaders().get("correlation")).isEqualTo("abc");
			assertThat(listener.message.getHeaders()).doesNotContainKey(KafkaHeaders.RECORD_METADATA);
			assertThat(listener.result).isInstanceOf(RecordMetadata.class);
			assertThat(((RecordMetadata) listener.result).topic()).isEqualTo("success-out");
			assertThat(listener.error).isNull();
		}
	}

	@Test
	void listenerIsNotifiedOfFailedSend() throws Exception {
		try (ConfigurableApplicationContext context = run("failure", FAIL_SENDS)) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			RecordingListener listener = context.getBean(RecordingListener.class);

			streamBridge.send("failure-out", MessageBuilder.withPayload("hello")
					.setHeader("correlation", "xyz").build());

			assertThat(listener.errorLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(listener.bindingName).isEqualTo("failure-out");
			assertThat(listener.message.getHeaders().get("correlation")).isEqualTo("xyz");
			assertThat(listener.error).isInstanceOf(TimeoutException.class).hasMessage("simulated send failure");
			assertThat(listener.result).isNull();
		}
	}

	@Test
	void recordMetadataChannelStillReceivesSuccess() throws Exception {
		try (ConfigurableApplicationContext context = run("metadata",
				"--spring.cloud.stream.kafka.bindings.metadata-out.producer.recordMetadataChannel=metadataChannel")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			RecordingListener listener = context.getBean(RecordingListener.class);

			streamBridge.send("metadata-out", "hello");

			assertThat(listener.successLatch.await(10, TimeUnit.SECONDS)).isTrue();
			QueueChannel metadataChannel = context.getBean("metadataChannel", QueueChannel.class);
			Message<?> received = metadataChannel.receive(10_000);
			assertThat(received).isNotNull();
			assertThat(received.getHeaders().get(KafkaHeaders.RECORD_METADATA)).isInstanceOf(RecordMetadata.class);
		}
	}

	@Test
	void errorChannelStillReceivesFailure() throws Exception {
		try (ConfigurableApplicationContext context = run("errors", FAIL_SENDS,
				"--spring.cloud.stream.bindings.errors-out.producer.errorChannelEnabled=true")) {
			StreamBridge streamBridge = context.getBean(StreamBridge.class);
			RecordingListener listener = context.getBean(RecordingListener.class);

			streamBridge.send("errors-out", "hello");

			assertThat(listener.errorLatch.await(10, TimeUnit.SECONDS)).isTrue();
			ErrorMessage errorMessage = context.getBean(ErrorCollector.class).errors.poll(10, TimeUnit.SECONDS);
			assertThat(errorMessage).isNotNull();
		}
	}

	private static final String[] FAIL_SENDS = {"--test.fail-sends=true"};

	private ConfigurableApplicationContext run(String name, String[] first, String... more) {
		String[] extra = new String[first.length + more.length];
		System.arraycopy(first, 0, extra, 0, first.length);
		System.arraycopy(more, 0, extra, first.length, more.length);
		return run(name, extra);
	}

	private ConfigurableApplicationContext run(String name, String... extra) {
		String[] common = {"--server.port=0", "--spring.jmx.enabled=false",
				"--spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
				"--spring.cloud.stream.bindings." + name + "-out.destination=" + name + "-out"};
		String[] all = new String[common.length + extra.length];
		System.arraycopy(common, 0, all, 0, common.length);
		System.arraycopy(extra, 0, all, common.length, extra.length);
		return new SpringApplicationBuilder(TestConfig.class).web(WebApplicationType.NONE).run(all);
	}

	static class RecordingListener implements SendResultListener {

		final CountDownLatch successLatch = new CountDownLatch(1);

		final CountDownLatch errorLatch = new CountDownLatch(1);

		volatile String bindingName;

		volatile Message<?> message;

		volatile Object result;

		volatile Throwable error;

		@Override
		public void onSuccess(String bindingName, Message<?> message, @Nullable Object result) {
			this.bindingName = bindingName;
			this.message = message;
			this.result = result;
			this.successLatch.countDown();
		}

		@Override
		public void onError(String bindingName, Message<?> message, Throwable exception) {
			this.bindingName = bindingName;
			this.message = message;
			this.error = exception;
			this.errorLatch.countDown();
		}

	}

	static class ErrorCollector {

		final BlockingQueue<ErrorMessage> errors = new LinkedBlockingQueue<>();

		@ServiceActivator(inputChannel = "errorChannel")
		void handle(ErrorMessage message) {
			this.errors.add(message);
		}

	}

	@EnableAutoConfiguration
	@Configuration
	public static class TestConfig {

		@Bean
		RecordingListener recordingListener() {
			return new RecordingListener();
		}

		/**
		 * Completes every send with a failure, as a broker rejecting the write would.
		 */
		@Bean
		ClientFactoryCustomizer failingProducers(Environment environment) {
			return new ClientFactoryCustomizer() {

				@Override
				@SuppressWarnings({"unchecked", "rawtypes"})
				public void configure(ProducerFactory<?, ?> pf) {
					if (environment.getProperty("test.fail-sends", Boolean.class, false)
							&& pf instanceof DefaultKafkaProducerFactory factory) {
						factory.addPostProcessor((producer) -> (Producer) Proxy.newProxyInstance(
								getClass().getClassLoader(), new Class<?>[] {Producer.class}, (proxy, method, args) -> {
									if ("send".equals(method.getName())) {
										TimeoutException failure = new TimeoutException("simulated send failure");
										if (args.length > 1 && args[1] instanceof Callback callback) {
											// Fail later, from another thread, as a broker rejecting the write would
											String topic = ((ProducerRecord<?, ?>) args[0]).topic();
											CompletableFuture.runAsync(() -> callback.onCompletion(
													new RecordMetadata(new TopicPartition(topic, 0), -1, -1, -1, -1, -1), failure));
										}
										return new CompletableFuture<>();
									}
									return method.invoke(producer, args);
								}));
					}
				}

			};
		}

		@Bean
		ErrorCollector errorCollector() {
			return new ErrorCollector();
		}

		@Bean
		MessageChannel metadataChannel() {
			return new QueueChannel();
		}

	}

}
