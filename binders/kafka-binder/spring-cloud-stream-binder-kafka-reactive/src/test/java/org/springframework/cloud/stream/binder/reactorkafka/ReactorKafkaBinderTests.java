/*
 * Copyright 2021-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.sender.SenderResult;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.FluxMessageChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.support.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 4.0
 *
 */
@EmbeddedKafka(topics = { "testCa", "testCb", "testC1", "testP" })
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

		CountDownLatch latch = new CountDownLatch(2);

		FluxMessageChannel inbound = new FluxMessageChannel();
		Subscriber<Message<?>> sub = new Subscriber<Message<?>>() {

			@Override
			public void onSubscribe(Subscription s) {
				s.request(2);
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
		props.setMultiplex(true);

		Binding<MessageChannel> consumer = binder.bindConsumer("testCa, testCb", "foo", inbound, props);

		DefaultKafkaProducerFactory pf =
				new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(EmbeddedKafkaCondition.getBroker()));
		KafkaTemplate kt = new KafkaTemplate<>(pf);
		kt.send("testCa", "foo").get(10, TimeUnit.SECONDS);
		kt.send("testCb", "bar").get(10, TimeUnit.SECONDS);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.unbind();
		pf.destroy();
	}

	@Test
	void concurrencyManual() throws Exception {
		concurrency(false);
	}

	@Test
	void concurrencyAtMostOnce() throws Exception {
		concurrency(true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	void concurrency(boolean atMostOnce) throws Exception {
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
		CountDownLatch messageLatch2 = new CountDownLatch(6);
		Set<Integer> partitions = new HashSet<>();
		List<String> payloads = Collections.synchronizedList(new ArrayList<>());

		FluxMessageChannel inbound = new FluxMessageChannel();
		Subscriber<Message<?>> sub = new Subscriber<Message<?>>() {

			@Override
			public void onSubscribe(Subscription s) {
				s.request(10);
				subscriptionLatch.countDown();
			}

			@Override
			public void onNext(Message<?> msg) {
				payloads.add((String) msg.getPayload());
				partitions.add(msg.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION, Integer.class));
				if (!atMostOnce) {
					msg.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, ReceiverOffset.class).acknowledge();
				}
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
		ext.setReactiveAtMostOnce(atMostOnce);
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
		Collections.sort(payloads);
		if (!atMostOnce) {
			assertThat(payloads).containsExactly("bar", "baz", "buz", "fiz", "foo", "qux");
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void autoCommit() throws Exception {
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
		Set<Integer> partitions = new HashSet<>();
		List<String> payloads = Collections.synchronizedList(new ArrayList<>());

		FluxMessageChannel inbound = new FluxMessageChannel();
		Subscriber<Message<?>> sub = new Subscriber<Message<?>>() {

			@Override
			public void onSubscribe(Subscription s) {
				s.request(10);
				subscriptionLatch.countDown();
			}

			@Override
			public void onNext(Message<?> msg) {
				((Message<Flux<ConsumerRecord<?, String>>>) msg).getPayload()
						.doOnNext(rec -> {
							payloads.add(rec.value());
							messageLatch1.countDown();
						})
						.subscribe();
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
		ext.setReactiveAutoCommit(true);
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
		consumer.unbind();
		pf.destroy();
		Collections.sort(payloads);
		assertThat(payloads).containsExactly("bar", "baz", "foo", "qux");
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
		CountDownLatch latch = new CountDownLatch(1);
		GenericApplicationContext context = new GenericApplicationContext();
		context.registerBean("sendResults", FluxMessageChannel.class);
		context.refresh();
		FluxMessageChannel results = context.getBean("sendResults", FluxMessageChannel.class);
		ConsumerEndpointFactoryBean fb = new ConsumerEndpointFactoryBean();
		AtomicReference<SenderResult<Integer>> senderResult = new AtomicReference<>();
		fb.setHandler(new MessageHandler() {

			@SuppressWarnings("unchecked")
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				senderResult.set((SenderResult<Integer>) message.getPayload());
				latch.countDown();
			}

		});
		fb.setInputChannel(results);
		fb.setBeanFactory(context.getBeanFactory());
		fb.afterPropertiesSet();
		fb.start();
		binder.setApplicationContext(context);
		@SuppressWarnings("rawtypes")
		ObjectProvider<SenderOptionsCustomizer> cust = mock(ObjectProvider.class);
		AtomicBoolean custCalled = new AtomicBoolean();
		given(cust.getIfUnique()).willReturn((name, opts) -> {
			custCalled.set(true);
			return opts;
		});
		binder.senderOptionsCustomizers(cust);

		MessageChannel outbound = new FluxMessageChannel();
		KafkaProducerProperties ext = new KafkaProducerProperties();
		ExtendedProducerProperties<KafkaProducerProperties> props =
				new ExtendedProducerProperties<KafkaProducerProperties>(ext);
		props.getExtension().setRecordMetadataChannel("sendResults");

		Binding<MessageChannel> bindProducer = binder.bindProducer("testP", outbound, props);
		AtomicReference<Mono<RecordMetadata>> sendResult = new AtomicReference<>();
		outbound.send(MessageBuilder.withPayload("foo")
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, 1)
				.build());
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(senderResult.get().correlationMetadata()).isEqualTo(1);
		bindProducer.unbind();
		assertThat(custCalled).isTrue();
	}

}
