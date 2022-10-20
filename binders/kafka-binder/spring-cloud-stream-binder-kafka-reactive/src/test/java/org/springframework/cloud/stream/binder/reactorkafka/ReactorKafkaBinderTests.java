/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.reactorkafka;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.support.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 4.0
 *
 */
@EmbeddedKafka(topics = { "testC", "testC1", "testP" })
public class ReactorKafkaBinderTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void consumerBinding() throws Exception {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.setBootstrapServers(
				Collections.singletonList(EmbeddedKafkaCondition.getBroker().getBrokersAsString()));
		KafkaBinderConfigurationProperties binderProps = new KafkaBinderConfigurationProperties(kafkaProperties);
		KafkaTopicProvisioner provisioner = new KafkaTopicProvisioner(binderProps, kafkaProperties, prop -> {
		});
		provisioner.setMetadataRetryOperations(new RetryTemplate());
		ReactorKafkaBinder binder = new ReactorKafkaBinder(binderProps, provisioner);
		binder.setApplicationContext(mock(GenericApplicationContext.class));

		CountDownLatch latch = new CountDownLatch(1);

		FluxMessageChannel inbound = new FluxMessageChannel();
		Subscriber<Message<?>> sub = new Subscriber<Message<?>>() {

			@Override
			public void onSubscribe(Subscription s) {
				s.request(1);
			}

			@Override
			public void onNext(Message<?> t) {
				latch.countDown();
			}

			@Override
			public void onError(Throwable t) {
			}

			@Override
			public void onComplete() {
			}

		};
		inbound.subscribe(sub);

		KafkaConsumerProperties ext = new KafkaConsumerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> props =
				new ExtendedConsumerProperties<KafkaConsumerProperties>(ext);

		Binding<MessageChannel> consumer = binder.bindConsumer("testC", "foo", inbound, props);

		DefaultKafkaProducerFactory pf =
				new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(EmbeddedKafkaCondition.getBroker()));
		KafkaTemplate kt = new KafkaTemplate<>(pf);
		kt.send("testC", "foo").get(10, TimeUnit.SECONDS);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.unbind();
		pf.destroy();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void concurrency() throws Exception {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.setBootstrapServers(
				Collections.singletonList(EmbeddedKafkaCondition.getBroker().getBrokersAsString()));
		KafkaBinderConfigurationProperties binderProps = new KafkaBinderConfigurationProperties(kafkaProperties);
		KafkaTopicProvisioner provisioner = new KafkaTopicProvisioner(binderProps, kafkaProperties, prop -> {
		});
		provisioner.setMetadataRetryOperations(new RetryTemplate());
		ReactorKafkaBinder binder = new ReactorKafkaBinder(binderProps, provisioner);
		binder.setApplicationContext(mock(GenericApplicationContext.class));

		CountDownLatch subscriptionLatch = new CountDownLatch(1);
		CountDownLatch messageLatch1 = new CountDownLatch(4);
		CountDownLatch messageLatch2 = new CountDownLatch(10);
		Set<Integer> partitions = new HashSet<>();

		FluxMessageChannel inbound = new FluxMessageChannel();
		Subscriber<Message<?>> sub = new Subscriber<Message<?>>() {

			@Override
			public void onSubscribe(Subscription s) {
				s.request(10);
				subscriptionLatch.countDown();
			}

			@Override
			public void onNext(Message<?> msg) {
				partitions.add(msg.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION, Integer.class));
				messageLatch1.countDown();
				messageLatch2.countDown();
			}

			@Override
			public void onError(Throwable t) {
			}

			@Override
			public void onComplete() {
			}

		};
		inbound.subscribe(sub);

		KafkaConsumerProperties ext = new KafkaConsumerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> props =
				new ExtendedConsumerProperties<KafkaConsumerProperties>(ext);
		props.setConcurrency(2);

		Binding<MessageChannel> consumer = binder.bindConsumer("testC1", "foo", inbound, props);

		assertThat(subscriptionLatch.await(10, TimeUnit.SECONDS)).isTrue();
		DefaultKafkaProducerFactory pf =
				new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(EmbeddedKafkaCondition.getBroker()));
		KafkaTemplate kt = new KafkaTemplate<>(pf);
		kt.send("testC1", 0, null, "foo").get(10, TimeUnit.SECONDS);
		kt.send("testC1", 1, null, "bar").get(10, TimeUnit.SECONDS);
		kt.send("testC1", 0, null, "baz").get(10, TimeUnit.SECONDS);
		kt.send("testC1", 1, null, "qux").get(10, TimeUnit.SECONDS);
		assertThat(messageLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.stop();
		consumer.start();
		kt.send("testC1", 0, null, "fiz").get(10, TimeUnit.SECONDS);
		kt.send("testC1", 1, null, "buz").get(10, TimeUnit.SECONDS);
		assertThat(messageLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(partitions).hasSize(2);
		consumer.unbind();
		pf.destroy();
	}

	@Test
	void producerBinding() throws InterruptedException {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.setBootstrapServers(
				Collections.singletonList(EmbeddedKafkaCondition.getBroker().getBrokersAsString()));
		KafkaBinderConfigurationProperties binderProps = new KafkaBinderConfigurationProperties(kafkaProperties);
		KafkaTopicProvisioner provisioner = new KafkaTopicProvisioner(binderProps, kafkaProperties, prop -> {
		});
		provisioner.setMetadataRetryOperations(new RetryTemplate());
		ReactorKafkaBinder binder = new ReactorKafkaBinder(binderProps, provisioner);
		binder.setApplicationContext(new GenericApplicationContext());

		MessageChannel outbound = new FluxMessageChannel();
		KafkaProducerProperties ext = new KafkaProducerProperties();
		ExtendedProducerProperties<KafkaProducerProperties> props =
				new ExtendedProducerProperties<KafkaProducerProperties>(ext);

		Binding<MessageChannel> bindProducer = binder.bindProducer("testP", outbound, props);
		AtomicReference<Mono<RecordMetadata>> sendResult = new AtomicReference<>();
		outbound.send(MessageBuilder.withPayload("foo")
				.setHeader("sendResult", sendResult)
				.build());
		CountDownLatch latch = new CountDownLatch(1);
		sendResult.get().doOnNext(rmd -> {
			latch.countDown();
		}).subscribe();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

}
