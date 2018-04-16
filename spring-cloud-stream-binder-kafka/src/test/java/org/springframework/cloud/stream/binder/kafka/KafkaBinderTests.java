/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.TestUtils;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.utils.KafkaTopicUtils;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.integration.kafka.support.KafkaSendFailureException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Soby Chacko
 * @author Ilayaperumal Gopinathan
 * @author Henryk Konsek
 * @author Gary Russell
 */
public class KafkaBinderTests extends
		PartitionCapableBinderTests<AbstractKafkaTestBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	private static final int DEFAULT_OPERATION_TIMEOUT = 30;

	@Rule
	public ExpectedException expectedProvisioningException = ExpectedException.none();

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 10, "error.pollableDlq.group");

	private KafkaTestBinder binder;

	private AdminClient adminClient;

	@Override
	protected ExtendedConsumerProperties<KafkaConsumerProperties> createConsumerProperties() {
		final ExtendedConsumerProperties<KafkaConsumerProperties> kafkaConsumerProperties = new ExtendedConsumerProperties<>(
				new KafkaConsumerProperties());
		// set the default values that would normally be propagated by Spring Cloud Stream
		kafkaConsumerProperties.setInstanceCount(1);
		kafkaConsumerProperties.setInstanceIndex(0);
		return kafkaConsumerProperties;
	}

	@Override
	protected ExtendedProducerProperties<KafkaProducerProperties> createProducerProperties() {
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());
		producerProperties.getExtension().setSync(true);
		return producerProperties;
	}

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Override
	protected KafkaTestBinder getBinder() {
		if (binder == null) {
			KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
			KafkaTopicProvisioner kafkaTopicProvisioner = new KafkaTopicProvisioner(binderConfiguration, new TestKafkaProperties());
			try {
				kafkaTopicProvisioner.afterPropertiesSet();
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
			binder = new KafkaTestBinder(binderConfiguration, kafkaTopicProvisioner);
		}
		return binder;
	}

	private Binder getBinder(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties) {
		KafkaTopicProvisioner provisioningProvider =
				new KafkaTopicProvisioner(kafkaBinderConfigurationProperties, new TestKafkaProperties());
		try {
			provisioningProvider.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return new KafkaTestBinder(kafkaBinderConfigurationProperties, provisioningProvider);
	}

	private KafkaBinderConfigurationProperties createConfigurationProperties() {
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties(
				new TestKafkaProperties());
		BrokerAddress[] brokerAddresses = embeddedKafka.getBrokerAddresses();
		List<String> bAddresses = new ArrayList<>();
		for (BrokerAddress bAddress : brokerAddresses) {
			bAddresses.add(bAddress.toString());
		}
		String[] foo = new String[bAddresses.size()];
		binderConfiguration.setBrokers(bAddresses.toArray(foo));
		return binderConfiguration;
	}

	private int partitionSize(String topic) {
		return consumerFactory().createConsumer().partitionsFor(topic).size();
	}

	private void invokeCreateTopic(String topic, int partitions, int replicationFactor) throws Throwable {

		NewTopic newTopic = new NewTopic(topic, partitions,
				(short) replicationFactor);
		CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
		topics.all().get(DEFAULT_OPERATION_TIMEOUT, TimeUnit.SECONDS);
	}

	private String getKafkaOffsetHeaderKey() {
		return KafkaHeaders.OFFSET;
	}

	@Before
	public void init() {
		String multiplier = System.getenv("KAFKA_TIMEOUT_MULTIPLIER");
		if (multiplier != null) {
			timeoutMultiplier = Double.parseDouble(multiplier);
		}

		BrokerAddress[] brokerAddresses = embeddedKafka.getBrokerAddresses();
		List<String> bAddresses = new ArrayList<>();
		for (BrokerAddress bAddress : brokerAddresses) {
			bAddresses.add(bAddress.toString());
		}
		String[] foo = new String[bAddresses.size()];

		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bAddresses.toArray(foo)[0]);
		adminClient = AdminClient.create(adminConfigs);
	}

	private int invokePartitionSize(String topic) throws Throwable {

		DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
		KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();
		Map<String, TopicDescription> stringTopicDescriptionMap = all.get(DEFAULT_OPERATION_TIMEOUT, TimeUnit.SECONDS);
		TopicDescription topicDescription = stringTopicDescriptionMap.get(topic);
		return topicDescription.partitions().size();
	}

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	public Spy spyOn(final String name) {
		throw new UnsupportedOperationException("'spyOn' is not used by Kafka tests");
	}

	private ConsumerFactory<byte[], byte[]> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getKafkaConnectionString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "TEST-CONSUMER-GROUP");
		Deserializer<byte[]> valueDecoder = new ByteArrayDeserializer();
		Deserializer<byte[]> keyDecoder = new ByteArrayDeserializer();

		return new DefaultKafkaConsumerFactory<>(props, keyDecoder, valueDecoder);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testTrustedPackages() throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output", producerBindingProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTrustedPackages(new String[]{"org.springframework.util"});

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<MessageChannel> producerBinding = binder.bindProducer("bar.0", moduleOutputChannel,
				producerBindingProperties.getProducer());

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bar.0",
				"testSendAndReceiveNoOriginalContentType", moduleInputChannel, consumerProperties);
		binderBindUnbindLatency();

		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
				.setHeader("foo", MimeTypeUtils.TEXT_PLAIN)
				.build();

		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");


		Assertions.assertThat(inboundMessageRef.get()).isNotNull();
		Assertions.assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo("foo");
		Assertions.assertThat(inboundMessageRef.get().getHeaders().get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE)).isNull();
		Assertions.assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		Assertions.assertThat(inboundMessageRef.get().getHeaders().get("foo")).isInstanceOf(MimeType.class);
		MimeType actual = (MimeType) inboundMessageRef.get().getHeaders().get("foo");
		Assertions.assertThat(actual).isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testSendAndReceiveNoOriginalContentType() throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));
		Binding<MessageChannel> producerBinding = binder.bindProducer("bar.0",
				moduleOutputChannel, producerBindingProperties.getProducer());

		consumerProperties.getExtension().setTrustedPackages(new String[]{"org.springframework.util"});
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("bar.0",
				"testSendAndReceiveNoOriginalContentType", moduleInputChannel,
				consumerProperties);
		binderBindUnbindLatency();

		//TODO: Will have to fix the MimeType to convert to byte array once this issue has been resolved:
		//https://github.com/spring-projects/spring-kafka/issues/424
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo("foo");
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testSendAndReceive() throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(
				createProducerProperties());
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				outputBindingProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.bar",
				moduleOutputChannel, outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.bar",
				"testSendAndReceive", moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload("foo".getBytes(StandardCharsets.UTF_8))
				.setHeader(MessageHeaders.CONTENT_TYPE,
						MimeTypeUtils.APPLICATION_OCTET_STREAM)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo("foo");
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE))
				.isEqualTo(MimeTypeUtils.APPLICATION_OCTET_STREAM);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDlqWithNativeSerializationEnabledOnDlqProducer() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();

		//Native serialization for producer
		producerProperties.setUseNativeEncoding(true);
		Map<String, String> producerConfig = new HashMap<>();
		producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.getExtension().setConfiguration(producerConfig);

		BindingProperties outputBindingProperties = createProducerBindingProperties(
				producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				outputBindingProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

		//Native Deserialization for consumer
		consumerProperties.setUseNativeDecoding(true);
		Map<String, String> consumerConfig = new HashMap<>();
		consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.getExtension().setConfiguration(consumerConfig);

		//Setting dlq producer properties on the consumer
		consumerProperties.getExtension().setDlqProducerProperties(producerProperties.getExtension());
		consumerProperties.getExtension().setEnableDlq(true);

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.bar",
				moduleOutputChannel, outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.bar",
				"testDlqWithNativeEncoding-1", moduleInputChannel, consumerProperties);

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();

		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);

		//Consumer for the DLQ destination
		QueueChannel dlqChannel = new QueueChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);

		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.foo.bar." + "testDlqWithNativeEncoding-1", null, dlqChannel, dlqConsumerProperties);
		binderBindUnbindLatency();

		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload("foo")
				.build();

		moduleOutputChannel.send(message);

		Message<?> receivedMessage = receive(dlqChannel, 5);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo("foo".getBytes());
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_ORIGINAL_TOPIC))
				.isEqualTo("foo.bar".getBytes(StandardCharsets.UTF_8));
		assertThat(new String((byte[]) receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_EXCEPTION_MESSAGE)))
				.startsWith("failed to send Message to channel 'input'");
		assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_EXCEPTION_STACKTRACE))
				.isNotNull();
		binderBindUnbindLatency();

		dlqConsumerBinding.unbind();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDlqWithNativeDecodingOnConsumerButMissingSerializerOnDlqProducer() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		//Native serialization for producer
		producerProperties.setUseNativeEncoding(true);
		Map<String, String> producerConfig = new HashMap<>();
		producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.getExtension().setConfiguration(producerConfig);
		BindingProperties outputBindingProperties = createProducerBindingProperties(
				producerProperties);
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				outputBindingProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		//Native Deserialization for consumer
		consumerProperties.setUseNativeDecoding(true);

		Map<String, String> consumerConfig = new HashMap<>();
		consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		//No Dlq producer properties set on the consumer with a native serializer. This should cause an error for DLQ sending.

		consumerProperties.getExtension().setConfiguration(consumerConfig);
		consumerProperties.getExtension().setEnableDlq(true);

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.bar",
				moduleOutputChannel, outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.bar",
				"testDlqWithNativeEncoding-2", moduleInputChannel, consumerProperties);

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();

		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);

		//Consumer for the DLQ destination
		QueueChannel dlqChannel = new QueueChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);

		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.foo.bar." + "testDlqWithNativeEncoding-2", null, dlqChannel, dlqConsumerProperties);
		binderBindUnbindLatency();

		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload("foo")
				.build();

		moduleOutputChannel.send(message);

		Message<?> receivedMessage = dlqChannel.receive(5000);
		//Ensure that we didn't receive anything on DLQ because of serializer config missing
		//on dlq producer while native Decoding is enabled.
		assertThat(receivedMessage).isNull();

		binderBindUnbindLatency();

		dlqConsumerBinding.unbind();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testDlqAndRetry() throws Exception {
		testDlqGuts(true, null);
	}

	@Test
	public void testDlq() throws Exception {
		testDlqGuts(false, null);
	}

	@Test
	public void testDlqNone() throws Exception {
		testDlqGuts(false, HeaderMode.none);
	}

	@Test
	public void testDlqEmbedded() throws Exception {
		testDlqGuts(false, HeaderMode.embeddedHeaders);
	}

	private void testDlqGuts(boolean withRetry, HeaderMode headerMode) throws Exception {
		AbstractKafkaTestBinder binder = getBinder();

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().setHeaderPatterns(new String[]{MessageHeaders.CONTENT_TYPE});
		producerProperties.setHeaderMode(headerMode);

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

		consumerProperties.setMaxAttempts(withRetry ? 2 : 1);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		consumerProperties.setHeaderMode(headerMode);

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		QueueChannel dlqChannel = new QueueChannel();
		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);

		long uniqueBindingId = System.currentTimeMillis();

		String producerName = "dlqTest." + uniqueBindingId + ".0";
		Binding<MessageChannel> producerBinding = binder.bindProducer(producerName,
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(producerName,
				"testGroup", moduleInputChannel, consumerProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);
		dlqConsumerProperties.setHeaderMode(headerMode);

		ApplicationContext context = TestUtils.getPropertyValue(binder.getBinder(), "applicationContext",
				ApplicationContext.class);
		SubscribableChannel boundErrorChannel = context
				.getBean(producerName + ".testGroup.errors-0", SubscribableChannel.class);
		SubscribableChannel globalErrorChannel = context.getBean("errorChannel", SubscribableChannel.class);
		final AtomicReference<Message<?>> boundErrorChannelMessage = new AtomicReference<>();
		final AtomicReference<Message<?>> globalErrorChannelMessage = new AtomicReference<>();
		final AtomicBoolean hasRecovererInCallStack = new AtomicBoolean(!withRetry);
		boundErrorChannel.subscribe(message -> {
			boundErrorChannelMessage.set(message);
			String stackTrace = Arrays.toString(new RuntimeException().getStackTrace());
			hasRecovererInCallStack.set(stackTrace.contains("ErrorMessageSendingRecoverer"));
		});
		globalErrorChannel.subscribe(globalErrorChannelMessage::set);

		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.dlqTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);
		binderBindUnbindLatency();
		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage = MessageBuilder.withPayload(testMessagePayload.getBytes()).build();
		moduleOutputChannel.send(testMessage);

		Message<?> receivedMessage = receive(dlqChannel, 3);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessagePayload.getBytes());
		if (HeaderMode.embeddedHeaders.equals(headerMode)) {
			assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
			assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_ORIGINAL_TOPIC))
					.isEqualTo(producerName);
			assertThat(((String) receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_EXCEPTION_MESSAGE)))
					.startsWith("failed to send Message to channel 'input'");
			assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_EXCEPTION_STACKTRACE))
					.isNotNull();
		}
		else if (!HeaderMode.none.equals(headerMode)) {
			assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
			assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_ORIGINAL_TOPIC))
					.isEqualTo(producerName.getBytes(StandardCharsets.UTF_8));
			assertThat(new String((byte[]) receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_EXCEPTION_MESSAGE)))
					.startsWith("failed to send Message to channel 'input'");
			assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_EXCEPTION_STACKTRACE))
					.isNotNull();
		}
		else {
			assertThat(receivedMessage.getHeaders().get(KafkaMessageChannelBinder.X_ORIGINAL_TOPIC)).isNull();
		}
		binderBindUnbindLatency();

		// verify we got a message on the dedicated error channel and the global (via bridge)
		assertThat(boundErrorChannelMessage.get()).isNotNull();
		assertThat(globalErrorChannelMessage.get()).isNotNull();
		assertThat(hasRecovererInCallStack.get()).isEqualTo(withRetry);

		dlqConsumerBinding.unbind();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultAutoCommitOnErrorWithoutDlq() throws Exception {
		Binder binder = getBinder();

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		BindingProperties producerBindingProperties = createProducerBindingProperties(
				producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);

		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage = MessageBuilder.withPayload(testMessagePayload.getBytes()).build();
		moduleOutputChannel.send(testMessage);

		assertThat(handler.getLatch().await((int) (timeoutMultiplier * 1000), TimeUnit.MILLISECONDS));
		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> receivedMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(receivedMessage).isNotNull();
		assertThat(new String((byte[])receivedMessage.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		consumerBinding.unbind();

		// on the second attempt the message is redelivered
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		binderBindUnbindLatency();
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage2 = MessageBuilder.withPayload(testMessage2Payload.getBytes()).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> firstReceived = receive(successfulInputChannel);
		assertThat(firstReceived.getPayload()).isEqualTo(testMessagePayload.getBytes());
		Message<?> secondReceived = receive(successfulInputChannel);
		assertThat(secondReceived.getPayload()).isEqualTo(testMessage2Payload.getBytes());
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultAutoCommitOnErrorWithDlq() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		BindingProperties producerBindingProperties = createProducerBindingProperties(
				producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);
		QueueChannel dlqChannel = new QueueChannel();
		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(
				"error.retryTest." + uniqueBindingId + ".0.testGroup", null, dlqChannel, dlqConsumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage = MessageBuilder.withPayload(testMessagePayload.getBytes()).build();
		moduleOutputChannel.send(testMessage);

		Message<?> dlqMessage = receive(dlqChannel, 3);
		assertThat(dlqMessage).isNotNull();
		assertThat(dlqMessage.getPayload()).isEqualTo(testMessagePayload.getBytes());

		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> handledMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(handledMessage).isNotNull();
		assertThat(new String((byte[])handledMessage.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		binderBindUnbindLatency();
		dlqConsumerBinding.unbind();
		consumerBinding.unbind();

		// on the second attempt the message is not redelivered because the DLQ is set
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage2 = MessageBuilder.withPayload(testMessage2Payload.getBytes()).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> receivedMessage = receive(successfulInputChannel);
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessage2Payload.getBytes());

		binderBindUnbindLatency();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConfigurableDlqName() throws Exception {
		Binder binder = getBinder();

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.setBackOffInitialInterval(100);
		consumerProperties.setBackOffMaxInterval(150);
		consumerProperties.getExtension().setEnableDlq(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		String dlqName = "dlqTest";
		consumerProperties.getExtension().setDlqName(dlqName);
		BindingProperties producerBindingProperties = createProducerBindingProperties(
				producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				producerBindingProperties);

		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		FailingInvocationCountingMessageHandler handler = new FailingInvocationCountingMessageHandler();
		moduleInputChannel.subscribe(handler);

		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("retryTest." + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0",
				"testGroup", moduleInputChannel, consumerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> dlqConsumerProperties = createConsumerProperties();
		dlqConsumerProperties.setMaxAttempts(1);
		QueueChannel dlqChannel = new QueueChannel();
		Binding<MessageChannel> dlqConsumerBinding = binder.bindConsumer(dlqName, null, dlqChannel,
				dlqConsumerProperties);

		String testMessagePayload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage = MessageBuilder.withPayload(testMessagePayload.getBytes()).build();
		moduleOutputChannel.send(testMessage);

		Message<?> dlqMessage = receive(dlqChannel, 3);
		assertThat(dlqMessage).isNotNull();
		assertThat(dlqMessage.getPayload()).isEqualTo(testMessagePayload.getBytes());

		// first attempt fails
		assertThat(handler.getReceivedMessages().entrySet()).hasSize(1);
		Message<?> handledMessage = handler.getReceivedMessages().entrySet().iterator().next().getValue();
		assertThat(handledMessage).isNotNull();
		assertThat(new String((byte[])handledMessage.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testMessagePayload);
		assertThat(handler.getInvocationCount()).isEqualTo(consumerProperties.getMaxAttempts());
		binderBindUnbindLatency();
		dlqConsumerBinding.unbind();
		consumerBinding.unbind();

		// on the second attempt the message is not redelivered because the DLQ is set
		QueueChannel successfulInputChannel = new QueueChannel();
		consumerBinding = binder.bindConsumer("retryTest." + uniqueBindingId + ".0", "testGroup",
				successfulInputChannel, consumerProperties);
		String testMessage2Payload = "test." + UUID.randomUUID().toString();
		Message<byte[]> testMessage2 = MessageBuilder.withPayload(testMessage2Payload.getBytes()).build();
		moduleOutputChannel.send(testMessage2);

		Message<?> receivedMessage = receive(successfulInputChannel);
		assertThat(receivedMessage.getPayload()).isEqualTo(testMessage2Payload.getBytes());

		binderBindUnbindLatency();
		consumerBinding.unbind();
		producerBinding.unbind();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateKafkaTopicName() {
		KafkaTopicUtils.validateTopicName("foo:bar");
	}

	@Test
	@SuppressWarnings("unchecked")
	//TODO: This test needs to be rethought - sending byte[] without explicit content type - yet being converted by the json converter
	public void testCompression() throws Exception {
		final KafkaProducerProperties.CompressionType[] codecs = new KafkaProducerProperties.CompressionType[]{
				KafkaProducerProperties.CompressionType.none, KafkaProducerProperties.CompressionType.gzip,
				KafkaProducerProperties.CompressionType.snappy};
		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		Binder binder = getBinder();
		for (KafkaProducerProperties.CompressionType codec : codecs) {
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
			producerProperties.getExtension().setCompressionType(
					KafkaProducerProperties.CompressionType.valueOf(codec.toString()));

			DirectChannel moduleOutputChannel = createBindableChannel("output",
					createProducerBindingProperties(producerProperties));

			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

			DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

			Binding<MessageChannel> producerBinding = binder.bindProducer("testCompression", moduleOutputChannel,
					producerProperties);
			Binding<MessageChannel> consumerBinding = binder.bindConsumer("testCompression", "test", moduleInputChannel,
					consumerProperties);
			Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
					.build();
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			CountDownLatch latch = new CountDownLatch(1);

			AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
			moduleInputChannel.subscribe(message1 -> {
				try {
					inboundMessageRef.set((Message<byte[]>) message1);
				}
				finally {
					latch.countDown();
				}
			});
			Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

			assertThat(inboundMessageRef.get()).isNotNull();
			assertThat(inboundMessageRef.get().getPayload()).containsExactly(testPayload);
			producerBinding.unbind();
			consumerBinding.unbind();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testEarliest() throws Exception {
		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			Binder binder = getBinder();
			BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
			DirectChannel output = createBindableChannel("output", producerBindingProperties);

			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);
			consumerProperties.getExtension().setStartOffset(KafkaConsumerProperties.StartOffset.earliest);
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);

			DirectChannel input1 = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

			String testTopicName = UUID.randomUUID().toString();
			producerBinding = binder.bindProducer(testTopicName, output, producerBindingProperties.getProducer());
			String testPayload1 = "foo-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload1.getBytes()));

			consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, consumerProperties);
			CountDownLatch latch = new CountDownLatch(1);
			AtomicReference<Message<byte[]>> inboundMessageRef1 = new AtomicReference<>();
			MessageHandler messageHandler = message1 -> {
				try {
					inboundMessageRef1.set((Message<byte[]>) message1);
				}
				finally {
					latch.countDown();
				}
			};
			input1.subscribe(messageHandler);
			Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");
			assertThat(inboundMessageRef1.get()).isNotNull();
			String testPayload2 = "foo-" + UUID.randomUUID().toString();
			input1.unsubscribe(messageHandler);
			output.send(new GenericMessage<>(testPayload2.getBytes()));

			CountDownLatch latch1 = new CountDownLatch(1);
			AtomicReference<Message<byte[]>> inboundMessageRef2 = new AtomicReference<>();
			input1.subscribe(message1 -> {
				try {
					inboundMessageRef2.set((Message<byte[]>) message1);
				}
				finally {
					latch1.countDown();
				}
			});
			Assert.isTrue(latch1.await(5, TimeUnit.SECONDS), "Failed to receive message");

			assertThat(inboundMessageRef2.get()).isNotNull();
			assertThat(new String(inboundMessageRef2.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload2);
			Thread.sleep(2000);
			producerBinding.unbind();
			consumerBinding.unbind();
		}
		finally {
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
			if (producerBinding != null) {
				producerBinding.unbind();
			}
		}
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testSendAndReceiveMultipleTopics() throws Exception {
		Binder binder = getBinder();

		DirectChannel moduleOutputChannel1 = createBindableChannel("output1",
				createProducerBindingProperties(createProducerProperties()));
		DirectChannel moduleOutputChannel2 = createBindableChannel("output2",
				createProducerBindingProperties(createProducerProperties()));

		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding1 = binder.bindProducer("foo.x", moduleOutputChannel1,
				createProducerProperties());
		Binding<MessageChannel> producerBinding2 = binder.bindProducer("foo.y", moduleOutputChannel2,
				createProducerProperties());

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer("foo.x", "test", moduleInputChannel,
				consumerProperties);
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer("foo.y", "test", moduleInputChannel,
				consumerProperties);

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = org.springframework.integration.support.MessageBuilder.withPayload(
				testPayload1.getBytes()).build();
		String testPayload2 = "foo" + UUID.randomUUID().toString();
		Message<?> message2 = org.springframework.integration.support.MessageBuilder.withPayload(
				testPayload2.getBytes()).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel1.send(message1);
		moduleOutputChannel2.send(message2);

		Message<?>[] messages = new Message[2];
		messages[0] = receive(moduleInputChannel);
		messages[1] = receive(moduleInputChannel);

		assertThat(messages[0]).isNotNull();
		assertThat(messages[1]).isNotNull();
		assertThat(messages).extracting("payload").containsExactlyInAnyOrder(testPayload1.getBytes(),
				testPayload2.getBytes());

		producerBinding1.unbind();
		producerBinding2.unbind();

		consumerBinding1.unbind();
		consumerBinding2.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testManualAckSucceedsWhenAutoCommitOffsetIsTurnedOff() throws Exception {
		Binder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(createProducerProperties()));
		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				"testManualAckSucceedsWhenAutoCommitOffsetIsTurnedOff", moduleOutputChannel,
				createProducerProperties());

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoCommitOffset(false);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				"testManualAckSucceedsWhenAutoCommitOffsetIsTurnedOff", "test", moduleInputChannel,
				consumerProperties);

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = org.springframework.integration.support.MessageBuilder.withPayload(
				testPayload1.getBytes()).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message1);

		Message<?> receivedMessage = receive(moduleInputChannel);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isNotNull();
		Acknowledgment acknowledgment = receivedMessage.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT,
				Acknowledgment.class);
		try {
			acknowledgment.acknowledge();
		}
		catch (Exception e) {
			fail("Acknowledge must not throw an exception");
		}
		finally {
			producerBinding.unbind();
			consumerBinding.unbind();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testManualAckIsNotPossibleWhenAutoCommitOffsetIsEnabledOnTheBinder() throws Exception {
		Binder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(createProducerProperties()));
		QueueChannel moduleInputChannel = new QueueChannel();

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				"testManualAckIsNotPossibleWhenAutoCommitOffsetIsEnabledOnTheBinder", moduleOutputChannel,
				createProducerProperties());

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				"testManualAckIsNotPossibleWhenAutoCommitOffsetIsEnabledOnTheBinder", "test", moduleInputChannel,
				consumerProperties);


		AbstractMessageListenerContainer<?, ?> container = TestUtils.getPropertyValue(consumerBinding,
				"lifecycle.messageListenerContainer", AbstractMessageListenerContainer.class);
		assertThat(container.getContainerProperties().getAckMode()).isEqualTo(AckMode.BATCH);

		String testPayload1 = "foo" + UUID.randomUUID().toString();
		Message<?> message1 = org.springframework.integration.support.MessageBuilder.withPayload(
				testPayload1.getBytes()).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message1);

		Message<?> receivedMessage = receive(moduleInputChannel);
		assertThat(receivedMessage).isNotNull();
		assertThat(receivedMessage.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT)).isNull();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testTwoRequiredGroups() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));

		String testDestination = "testDestination" + UUID.randomUUID().toString().replace("-", "");

		producerProperties.setRequiredGroups("test1", "test2");
		Binding<MessageChannel> producerBinding = binder.bindProducer(testDestination, output, producerProperties);

		String testPayload = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload.getBytes()));

		QueueChannel inbound1 = new QueueChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		consumerProperties.getExtension().setAckEachRecord(true);
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(testDestination, "test1", inbound1,
				consumerProperties);
		QueueChannel inbound2 = new QueueChannel();
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(testDestination, "test2", inbound2,
				consumerProperties);

		AbstractMessageListenerContainer<?, ?> container = TestUtils.getPropertyValue(consumerBinding2,
				"lifecycle.messageListenerContainer", AbstractMessageListenerContainer.class);
		assertThat(container.getContainerProperties().getAckMode()).isEqualTo(AckMode.RECORD);

		Message<?> receivedMessage1 = receive(inbound1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String((byte[]) receivedMessage1.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload);
		Message<?> receivedMessage2 = receive(inbound2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String((byte[]) receivedMessage2.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload);

		consumerBinding1.unbind();
		consumerBinding2.unbind();
		producerBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testPartitionedModuleSpEL() throws Exception {
		Binder binder = getBinder();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setPartitioned(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0S");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("part.0", "test", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("part.0", "test", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("part.0", "test", input2, consumerProperties);

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		producerProperties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("hashCode()"));
		producerProperties.setPartitionCount(3);

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("part.0", output, producerProperties);
		try {
			Object endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint))
					.contains(getExpectedRoutingBaseDestination("part.0", "test") + "-' + headers['partition']");
		}
		catch (UnsupportedOperationException ignored) {
		}

		Message<Integer> message2 = org.springframework.integration.support.MessageBuilder.withPayload(2)
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "foo")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43).build();
		output.send(message2);
		output.send(new GenericMessage<>(1));
		output.send(new GenericMessage<>(0));

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		Condition<Message<?>> correlationHeadersForPayload2 = new Condition<Message<?>>() {

			@Override
			public boolean matches(Message<?> value) {
				IntegrationMessageHeaderAccessor accessor = new IntegrationMessageHeaderAccessor(value);
				return "foo".equals(accessor.getCorrelationId()) && 42 == accessor.getSequenceNumber()
						&& 43 == accessor.getSequenceSize();
			}
		};

		ObjectMapper om = new ObjectMapper();

		if (usesExplicitRouting()) {
			assertThat(om.readValue((byte[]) receive0.getPayload(), Integer.class)).isEqualTo(0);
			assertThat(om.readValue((byte[]) receive1.getPayload(), Integer.class)).isEqualTo(1);
			assertThat(om.readValue((byte[]) receive2.getPayload(), Integer.class)).isEqualTo(2);
			assertThat(receive2).has(correlationHeadersForPayload2);
		}
		else {
			List<Message<?>> receivedMessages = Arrays.asList(receive0, receive1, receive2);
			assertThat(receivedMessages).extracting("payload").containsExactlyInAnyOrder(new byte[]{48}, new byte[]{49}, new byte[]{50});
			Condition<Message<?>> payloadIs2 = new Condition<Message<?>>() {

				@Override
				public boolean matches(Message<?> value) {
					try {
						return om.readValue((byte[]) value.getPayload(), Integer.class).equals(2);
					}
					catch (IOException e) {
						//
					}
					return false;
				}
			};
			assertThat(receivedMessages).filteredOn(payloadIs2).areExactly(1, correlationHeadersForPayload2);

		}
		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testPartitionedModuleJava() throws Exception {
		Binder binder = getBinder();

		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceCount(4);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setPartitioned(true);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partJ.0", "test", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partJ.0", "test", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("partJ.0", "test", input2, consumerProperties);
		consumerProperties.setInstanceIndex(3);
		QueueChannel input3 = new QueueChannel();
		input3.setBeanName("test.input3J");
		Binding<MessageChannel> input3Binding = binder.bindConsumer("partJ.0", "test", input3, consumerProperties);

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionKeyExtractorClass(PartitionTestSupport.class);
		producerProperties.setPartitionSelectorClass(PartitionTestSupport.class);
		producerProperties.setPartitionCount(3); // overridden to 8 on the actual topic
		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(producerProperties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.0", output, producerProperties);
		if (usesExplicitRouting()) {
			Object endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint))
					.contains(getExpectedRoutingBaseDestination("partJ.0", "test") + "-' + headers['partition']");
		}

		output.send(new GenericMessage<>(2));
		output.send(new GenericMessage<>(1));
		output.send(new GenericMessage<>(0));
		output.send(new GenericMessage<>(3));

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();
		Message<?> receive3 = receive(input3);
		assertThat(receive3).isNotNull();
		ObjectMapper om = new ObjectMapper();

		assertThat(om.readValue((byte[]) receive0.getPayload(), Integer.class)).isEqualTo(0);
		assertThat(om.readValue((byte[]) receive1.getPayload(), Integer.class)).isEqualTo(1);
		assertThat(om.readValue((byte[]) receive2.getPayload(), Integer.class)).isEqualTo(2);
		assertThat(om.readValue((byte[]) receive3.getPayload(), Integer.class)).isEqualTo(3);

		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		input3Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings("unchecked")
	public void testAnonymousGroup() throws Exception {
		Binder binder = getBinder();
		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel output = createBindableChannel("output", producerBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer("defaultGroup.0", output,
				producerBindingProperties.getProducer());

		QueueChannel input1 = new QueueChannel();
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> binding1 = binder.bindConsumer("defaultGroup.0", null, input1,
				consumerProperties);

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer("defaultGroup.0", null, input2,
				consumerProperties);
		// Since we don't provide any topic info, let Kafka bind the consumer successfully
		Thread.sleep(1000);
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload1);

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload1);

		binding2.unbind();

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload2.getBytes()));

		binding2 = binder.bindConsumer("defaultGroup.0", null, input2, consumerProperties);
		// Since we don't provide any topic info, let Kafka bind the consumer successfully
		Thread.sleep(1000);
		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload3.getBytes()));

		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload2);
		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload(), StandardCharsets.UTF_8)).isNotNull();

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload3);

		producerBinding.unbind();
		binding1.unbind();
		binding2.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPartitionedModuleJavaWithRawMode() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.setHeaderMode(HeaderMode.none);
		properties.setPartitionKeyExtractorClass(RawKafkaPartitionTestSupport.class);
		properties.setPartitionSelectorClass(RawKafkaPartitionTestSupport.class);
		properties.setPartitionCount(6);

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(properties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("partJ.raw.0", output, properties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setPartitioned(true);
		consumerProperties.setHeaderMode(HeaderMode.none);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0J");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("partJ.raw.0", "test", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1J");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("partJ.raw.0", "test", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2J");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("partJ.raw.0", "test", input2, consumerProperties);

		output.send(new GenericMessage<>(new byte[]{(byte) 0}));
		output.send(new GenericMessage<>(new byte[]{(byte) 1}));
		output.send(new GenericMessage<>(new byte[]{(byte) 2}));

		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();

		assertThat(Arrays.asList(((byte[]) receive0.getPayload())[0], ((byte[]) receive1.getPayload())[0],
				((byte[]) receive2.getPayload())[0])).containsExactlyInAnyOrder((byte) 0, (byte) 1, (byte) 2);

		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPartitionedModuleSpELWithRawMode() throws Exception {
		Binder binder = getBinder();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload[0]"));
		properties.setPartitionSelectorExpression(spelExpressionParser.parseExpression("hashCode()"));
		properties.setPartitionCount(6);
		properties.setHeaderMode(HeaderMode.none);

		DirectChannel output = createBindableChannel("output", createProducerBindingProperties(properties));
		output.setBeanName("test.output");
		Binding<MessageChannel> outputBinding = binder.bindProducer("part.raw.0", output, properties);
		try {
			Object endpoint = extractEndpoint(outputBinding);
			assertThat(getEndpointRouting(endpoint))
					.contains(getExpectedRoutingBaseDestination("part.raw.0", "test") + "-' + headers['partition']");
		}
		catch (UnsupportedOperationException ignored) {
		}

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.setInstanceIndex(0);
		consumerProperties.setInstanceCount(3);
		consumerProperties.setPartitioned(true);
		consumerProperties.setHeaderMode(HeaderMode.none);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		QueueChannel input0 = new QueueChannel();
		input0.setBeanName("test.input0S");
		Binding<MessageChannel> input0Binding = binder.bindConsumer("part.raw.0", "test", input0, consumerProperties);
		consumerProperties.setInstanceIndex(1);
		QueueChannel input1 = new QueueChannel();
		input1.setBeanName("test.input1S");
		Binding<MessageChannel> input1Binding = binder.bindConsumer("part.raw.0", "test", input1, consumerProperties);
		consumerProperties.setInstanceIndex(2);
		QueueChannel input2 = new QueueChannel();
		input2.setBeanName("test.input2S");
		Binding<MessageChannel> input2Binding = binder.bindConsumer("part.raw.0", "test", input2, consumerProperties);

		Message<byte[]> message2 = org.springframework.integration.support.MessageBuilder.withPayload(new byte[]{2})
				.setHeader(IntegrationMessageHeaderAccessor.CORRELATION_ID, "kafkaBinderTestCommonsDelegate")
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER, 42)
				.setHeader(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, 43).build();
		output.send(message2);
		output.send(new GenericMessage<>(new byte[]{1}));
		output.send(new GenericMessage<>(new byte[]{0}));
		Message<?> receive0 = receive(input0);
		assertThat(receive0).isNotNull();
		Message<?> receive1 = receive(input1);
		assertThat(receive1).isNotNull();
		Message<?> receive2 = receive(input2);
		assertThat(receive2).isNotNull();
		assertThat(Arrays.asList(((byte[]) receive0.getPayload())[0], ((byte[]) receive1.getPayload())[0],
				((byte[]) receive2.getPayload())[0])).containsExactlyInAnyOrder((byte) 0, (byte) 1, (byte) 2);
		input0Binding.unbind();
		input1Binding.unbind();
		input2Binding.unbind();
		outputBinding.unbind();
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testSendAndReceiveWithRawMode() throws Exception {
		Binder binder = getBinder();

		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setHeaderMode(HeaderMode.none);
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setHeaderMode(HeaderMode.none);
		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));
		Binding<MessageChannel> producerBinding = binder.bindProducer("raw.0", moduleOutputChannel,
				producerProperties);

		Binding<MessageChannel> consumerBinding = binder.bindConsumer("raw.0", "test", moduleInputChannel,
				consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder
				.withPayload("testSendAndReceiveWithRawMode".getBytes()).build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo("testSendAndReceiveWithRawMode");
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testProducerErrorChannel() throws Exception {
		AbstractKafkaTestBinder binder = getBinder();
		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		ExtendedProducerProperties<KafkaProducerProperties> producerProps = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());
		producerProps.setHeaderMode(HeaderMode.none);
		producerProps.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer("ec.0", moduleOutputChannel, producerProps);
		final Message<?> message = MessageBuilder.withPayload("bad").setHeader(MessageHeaders.CONTENT_TYPE, "application/json")
				.build();
		SubscribableChannel ec = binder.getApplicationContext().getBean("ec.0.errors", SubscribableChannel.class);
		final AtomicReference<Message<?>> errorMessage = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(2);
		ec.subscribe(message1 -> {
			errorMessage.set(message1);
			latch.countDown();
		});
		SubscribableChannel globalEc = binder.getApplicationContext()
				.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, SubscribableChannel.class);
		globalEc.subscribe(message12 -> latch.countDown());
		KafkaProducerMessageHandler endpoint = TestUtils.getPropertyValue(producerBinding, "lifecycle",
				KafkaProducerMessageHandler.class);
		final RuntimeException fooException = new RuntimeException("foo");
		final AtomicReference<Object> sent = new AtomicReference<>();
		new DirectFieldAccessor(endpoint).setPropertyValue("kafkaTemplate",
				new KafkaTemplate(mock(ProducerFactory.class)) {

					@Override // SIK < 2.3
					public ListenableFuture<SendResult> send(String topic, Object payload) {
						sent.set(payload);
						SettableListenableFuture<SendResult> future = new SettableListenableFuture<>();
						future.setException(fooException);
						return future;
					}

					@Override // SIK 2.3+
					public ListenableFuture send(ProducerRecord record) {
						sent.set(record.value());
						SettableListenableFuture<SendResult> future = new SettableListenableFuture<>();
						future.setException(fooException);
						return future;
					}

				});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorMessage.get()).isInstanceOf(ErrorMessage.class);
		assertThat(errorMessage.get().getPayload()).isInstanceOf(KafkaSendFailureException.class);
		KafkaSendFailureException exception = (KafkaSendFailureException) errorMessage.get().getPayload();
		assertThat(exception.getCause()).isSameAs(fooException);
		assertThat(new String((byte[]) exception.getFailedMessage().getPayload(), StandardCharsets.UTF_8)).isEqualTo(message.getPayload());
		assertThat(exception.getRecord().value()).isSameAs(sent.get());
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoCreateTopicsEnabledSucceeds() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		configurationProperties.setAutoCreateTopics(true);
		Binder binder = getBinder(configurationProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String testTopicName = "nonexisting" + System.currentTimeMillis();
		DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<?> binding = binder.bindConsumer(testTopicName, "test", moduleInputChannel, consumerProperties);
		binding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomPartitionCountOverridesDefaultIfLarger() throws Exception {
		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(10);
		Binder binder = getBinder(binderConfiguration);
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(10);
		producerProperties.setPartitionKeyExpression(new LiteralExpression("foo"));

		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);

		assertThat(partitionSize("foo" + uniqueBindingId + ".0")).isEqualTo(10);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomPartitionCountDoesNotOverridePartitioningIfSmaller() throws Exception {
		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(6);
		Binder binder = getBinder(binderConfiguration);
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Thread.sleep(1000);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);

		assertThat(partitionSize("foo" + uniqueBindingId + ".0")).isEqualTo(6);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDynamicKeyExpression() throws Exception {
		Binder binder = getBinder(createConfigurationProperties());
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().getConfiguration().put("key.serializer", StringSerializer.class.getName());
		producerProperties.getExtension().setMessageKeyExpression(spelExpressionParser.parseExpression("headers.key"));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		String uniqueBindingId = UUID.randomUUID().toString();
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Thread.sleep(1000);
		Message<?> message = MessageBuilder.withPayload("somePayload").setHeader("key", "myDynamicKey").build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		String receivedKey = new String(inbound.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, byte[].class));
		assertThat(receivedKey).isEqualTo("myDynamicKey");
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomPartitionCountOverridesPartitioningIfLarger() throws Exception {
		byte[] testPayload = new byte[2048];
		Arrays.fill(testPayload, (byte) 65);
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setMinPartitionCount(4);
		Binder binder = getBinder(binderConfiguration);

		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setPartitionCount(5);
		producerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));
		DirectChannel moduleOutputChannel = createBindableChannel("output",
				createProducerBindingProperties(producerProperties));
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		long uniqueBindingId = System.currentTimeMillis();
		Binding<MessageChannel> producerBinding = binder.bindProducer("foo" + uniqueBindingId + ".0",
				moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo" + uniqueBindingId + ".0", null,
				moduleInputChannel, consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder.withPayload(testPayload)
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertThat((byte[]) inbound.getPayload()).containsExactly(testPayload);
		assertThat(partitionSize("foo" + uniqueBindingId + ".0")).isEqualTo(5);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDefaultConsumerStartsAtEarliest() throws Exception {
		Binder binder = getBinder(createConfigurationProperties());
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel output = createBindableChannel("output", producerBindingProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);

		DirectChannel input1 = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		String testTopicName = UUID.randomUUID().toString();
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output,
				createProducerProperties());
		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload1.getBytes()));

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
				consumerProperties);

		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef1 = new AtomicReference<>();
		MessageHandler messageHandler = message1 -> {
			try {
				inboundMessageRef1.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		};
		input1.subscribe(messageHandler);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");
		assertThat(inboundMessageRef1.get()).isNotNull();
		assertThat(new String(inboundMessageRef1.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload1);

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		input1.unsubscribe(messageHandler);
		output.send(new GenericMessage<>(testPayload2.getBytes()));

		CountDownLatch latch1 = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef2 = new AtomicReference<>();
		input1.subscribe(message1 -> {
			try {
				inboundMessageRef2.set((Message<byte[]>) message1);
			}
			finally {
				latch1.countDown();
			}
		});
		Assert.isTrue(latch1.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef2.get()).isNotNull();
		assertThat(new String(inboundMessageRef2.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload2);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testResume() throws Exception {
		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			Binder binder = getBinder(configurationProperties);

			BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
			DirectChannel output = createBindableChannel("output", producerBindingProperties);

			DirectChannel input1 = createBindableChannel("input", createConsumerBindingProperties(createConsumerProperties()));

			String testTopicName = UUID.randomUUID().toString();
			producerBinding = binder.bindProducer(testTopicName, output,
					producerBindingProperties.getProducer());
			ExtendedConsumerProperties<KafkaConsumerProperties> firstConsumerProperties = createConsumerProperties();
			consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1,
					firstConsumerProperties);
			CountDownLatch latch = new CountDownLatch(1);
			AtomicReference<Message<byte[]>> inboundMessageRef1 = new AtomicReference<>();
			MessageHandler messageHandler = message1 -> {
				try {
					inboundMessageRef1.set((Message<byte[]>) message1);
				}
				finally {
					latch.countDown();
				}
			};
			input1.subscribe(messageHandler);
			String testPayload1 = "foo1-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload1));
			Assert.isTrue(latch.await(15, TimeUnit.SECONDS), "Failed to receive message");

			assertThat(inboundMessageRef1.get()).isNotNull();
			assertThat(inboundMessageRef1.get().getPayload()).isNotNull();
			input1.unsubscribe(messageHandler);
			CountDownLatch latch1 = new CountDownLatch(1);
			AtomicReference<Message<byte[]>> inboundMessageRef2 = new AtomicReference<>();
			MessageHandler messageHandler1 = message1 -> {
				try {
					inboundMessageRef2.set((Message<byte[]>) message1);
				}
				finally {
					latch1.countDown();
				}
			};
			input1.subscribe(messageHandler1);
			String testPayload2 = "foo2-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload2.getBytes()));
			Assert.isTrue(latch1.await(15, TimeUnit.SECONDS), "Failed to receive message");
			assertThat(inboundMessageRef2.get()).isNotNull();
			assertThat(inboundMessageRef2.get().getPayload()).isNotNull();
			consumerBinding.unbind();

			Thread.sleep(2000);
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerBinding = binder.bindConsumer(testTopicName, "startOffsets", input1, consumerProperties);
			input1.unsubscribe(messageHandler1);
			CountDownLatch latch2 = new CountDownLatch(1);
			AtomicReference<Message<byte[]>> inboundMessageRef3 = new AtomicReference<>();
			MessageHandler messageHandler2 = message1 -> {
				try {
					inboundMessageRef3.set((Message< byte[]>) message1);
				}
				finally {
					latch2.countDown();
				}
			};
			input1.subscribe(messageHandler2);
			String testPayload3 = "foo3-" + UUID.randomUUID().toString();
			output.send(new GenericMessage<>(testPayload3.getBytes()));
			Assert.isTrue(latch2.await(15, TimeUnit.SECONDS), "Failed to receive message");
			assertThat(inboundMessageRef3.get()).isNotNull();
			assertThat(new String(inboundMessageRef3.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload3);
		}
		finally {
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
			if (producerBinding != null) {
				producerBinding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSyncProducerMetadata() throws Exception {
		Binder binder = getBinder(createConfigurationProperties());
		DirectChannel output = new DirectChannel();
		String testTopicName = UUID.randomUUID().toString();
		ExtendedProducerProperties<KafkaProducerProperties> properties = createProducerProperties();
		properties.getExtension().setSync(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output, properties);
		DirectFieldAccessor accessor = new DirectFieldAccessor(extractEndpoint(producerBinding));
		KafkaProducerMessageHandler wrappedInstance = (KafkaProducerMessageHandler) accessor.getWrappedInstance();
		assertThat(new DirectFieldAccessor(wrappedInstance).getPropertyValue("sync").equals(Boolean.TRUE))
				.withFailMessage("Kafka Sync Producer should have been enabled.");
		producerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoCreateTopicsDisabledOnBinderStillWorksAsLongAsBrokerCreatesTopic() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		configurationProperties.setAutoCreateTopics(false);
		Binder binder = getBinder(configurationProperties);
		BindingProperties producerBindingProperties = createProducerBindingProperties(createProducerProperties());
		DirectChannel output = createBindableChannel("output", producerBindingProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

		DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		String testTopicName = "createdByBroker-" + System.currentTimeMillis();

		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, output,
				producerBindingProperties.getProducer());

		String testPayload = "foo1-" + UUID.randomUUID().toString();
		output.send(new GenericMessage<>(testPayload));

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
		input.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo(testPayload);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoConfigureTopicsDisabledSucceedsIfTopicExisting() throws Throwable {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(testTopicName, 5, 1);
		configurationProperties.setAutoCreateTopics(false);
		Binder binder = getBinder(configurationProperties);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

		DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));
		Binding<MessageChannel> binding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
		binding.unbind();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPartitionCountIncreasedIfAutoAddPartitionsSet() throws Throwable {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		String testTopicName = "existing" + System.currentTimeMillis();
		configurationProperties.setMinPartitionCount(6);
		configurationProperties.setAutoAddPartitions(true);
		Binder binder = getBinder(configurationProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<?> binding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
		binding.unbind();
		assertThat(invokePartitionSize(testTopicName)).isEqualTo(6);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoAddPartitionsDisabledSucceedsIfTopicUnderPartitionedAndAutoRebalanceEnabled() throws Throwable {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(testTopicName, 1, 1);
		configurationProperties.setAutoAddPartitions(false);
		Binder binder = getBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

		DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);
		Binding binding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
		binding.unbind();
		assertThat(invokePartitionSize(testTopicName)).isEqualTo(1);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoAddPartitionsDisabledFailsIfTopicUnderPartitionedAndAutoRebalanceDisabled() throws Throwable {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(testTopicName, 1, 1);
		configurationProperties.setAutoAddPartitions(false);
		Binder binder = getBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel output = createBindableChannel("output", createConsumerBindingProperties(consumerProperties));
		// this consumer must consume from partition 2
		consumerProperties.setInstanceCount(3);
		consumerProperties.setInstanceIndex(2);
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		expectedProvisioningException.expect(ProvisioningException.class);
		expectedProvisioningException
				.expectMessage("The number of expected partitions was: 3, but 1 has been found instead");
		Binding binding = binder.bindConsumer(testTopicName, "test", output, consumerProperties);
		if (binding != null) {
			binding.unbind();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAutoAddPartitionsDisabledSucceedsIfTopicPartitionedCorrectly() throws Throwable {
		Binding<?> binding = null;
		try {
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

			String testTopicName = "existing" + System.currentTimeMillis();
			invokeCreateTopic(testTopicName, 6, 1);
			configurationProperties.setAutoAddPartitions(false);
			Binder binder = getBinder(configurationProperties);
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();

			DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

			// this consumer must consume from partition 2
			consumerProperties.setInstanceCount(3);
			consumerProperties.setInstanceIndex(2);
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);

			binding = binder.bindConsumer(testTopicName, "test-x", input, consumerProperties);

			TopicPartitionInitialOffset[] listenedPartitions = TestUtils.getPropertyValue(binding,
					"lifecycle.messageListenerContainer.containerProperties.topicPartitions",
					TopicPartitionInitialOffset[].class);
			assertThat(listenedPartitions).hasSize(2);
			assertThat(listenedPartitions).contains(new TopicPartitionInitialOffset(testTopicName, 2),
					new TopicPartitionInitialOffset(testTopicName, 5));
			int partitions = invokePartitionSize(testTopicName);
			assertThat(partitions).isEqualTo(6);
		}
		finally {
			if (binding != null) {
				binding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testPartitionCountNotReduced() throws Throwable {
		String testTopicName = "existing" + System.currentTimeMillis();

		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();

		invokeCreateTopic(testTopicName, 6, 1);
		configurationProperties.setAutoAddPartitions(true);
		Binder binder = getBinder(configurationProperties);
		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

		Binding<?> binding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
		binding.unbind();

		assertThat(partitionSize(testTopicName)).isEqualTo(6);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConsumerDefaultDeserializer() throws Throwable {
		Binding<?> binding = null;
		try {
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			String testTopicName = "existing" + System.currentTimeMillis();
			invokeCreateTopic(testTopicName, 5, 1);
			configurationProperties.setAutoCreateTopics(false);
			Binder binder = getBinder(configurationProperties);

			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

			binding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
			DirectFieldAccessor consumerAccessor = new DirectFieldAccessor(getKafkaConsumer(binding));
			assertTrue(consumerAccessor.getPropertyValue("keyDeserializer") instanceof ByteArrayDeserializer);
			assertTrue(consumerAccessor.getPropertyValue("valueDeserializer") instanceof ByteArrayDeserializer);
		}
		finally {
			if (binding != null) {
				binding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConsumerCustomDeserializer() throws Exception {
		Binding<?> binding = null;
		try {
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			Map<String, String> propertiesToOverride = configurationProperties.getConfiguration();
			propertiesToOverride.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			propertiesToOverride.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
			configurationProperties.setConfiguration(propertiesToOverride);
			String testTopicName = "existing" + System.currentTimeMillis();
			configurationProperties.setAutoCreateTopics(false);
			Binder binder = getBinder(configurationProperties);

			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			DirectChannel input = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

			binding = binder.bindConsumer(testTopicName, "test", input, consumerProperties);
			DirectFieldAccessor consumerAccessor = new DirectFieldAccessor(getKafkaConsumer(binding));
			assertTrue("Expected StringDeserializer as a custom key deserializer",
					consumerAccessor.getPropertyValue("keyDeserializer") instanceof StringDeserializer);
			assertTrue("Expected LongDeserializer as a custom value deserializer",
					consumerAccessor.getPropertyValue("valueDeserializer") instanceof LongDeserializer);
		}
		finally {
			if (binding != null) {
				binding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testNativeSerializationWithCustomSerializerDeserializer() throws Exception {
		Binding<?> producerBinding = null;
		Binding<?> consumerBinding = null;
		try {
			Integer testPayload = 10;
			Message<?> message = MessageBuilder.withPayload(testPayload).build();
			SubscribableChannel moduleOutputChannel = new DirectChannel();
			String testTopicName = "existing" + System.currentTimeMillis();
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			configurationProperties.setAutoAddPartitions(true);
			Binder binder = getBinder(configurationProperties);
			QueueChannel moduleInputChannel = new QueueChannel();
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
			producerProperties.setUseNativeEncoding(true);
			producerProperties.getExtension().getConfiguration().put("value.serializer",
					"org.apache.kafka.common.serialization.IntegerSerializer");
			producerBinding = binder.bindProducer(testTopicName, moduleOutputChannel, producerProperties);
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);
			consumerProperties.getExtension().getConfiguration().put("value.deserializer",
					"org.apache.kafka.common.serialization.IntegerDeserializer");
			consumerProperties.getExtension().setStandardHeaders(KafkaConsumerProperties.StandardHeaders.both);
			consumerBinding = binder.bindConsumer(testTopicName, "test", moduleInputChannel, consumerProperties);
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(moduleInputChannel, 500);
			assertThat(inbound).isNotNull();
			assertThat(inbound.getPayload()).isEqualTo(10);
			assertThat(inbound.getHeaders()).doesNotContainKey("contentType");
			assertThat(inbound.getHeaders().getId()).isNotNull();
			assertThat(inbound.getHeaders().getTimestamp()).isNotNull();
		}
		finally {
			if (producerBinding != null) {
				producerBinding.unbind();
			}
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
		}
	}

	private KafkaConsumer getKafkaConsumer(Binding binding) {
		DirectFieldAccessor bindingAccessor = new DirectFieldAccessor(binding);
		KafkaMessageDrivenChannelAdapter adapter = (KafkaMessageDrivenChannelAdapter) bindingAccessor
				.getPropertyValue("lifecycle");
		DirectFieldAccessor adapterAccessor = new DirectFieldAccessor(adapter);
		ConcurrentMessageListenerContainer messageListenerContainer =
				(ConcurrentMessageListenerContainer) adapterAccessor.getPropertyValue("messageListenerContainer");
		DirectFieldAccessor containerAccessor = new DirectFieldAccessor(messageListenerContainer);
		DefaultKafkaConsumerFactory consumerFactory = (DefaultKafkaConsumerFactory) containerAccessor
				.getPropertyValue("consumerFactory");
		return (KafkaConsumer) consumerFactory.createConsumer();
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testNativeSerializationWithCustomSerializerDeserializerBytesPayload() throws Exception {
		Binding<?> producerBinding = null;
		Binding<?> consumerBinding = null;
		try {
			byte[] testPayload = new byte[1];
			Message<?> message = MessageBuilder.withPayload(testPayload)
					.setHeader(MessageHeaders.CONTENT_TYPE, "something/funky")
					.build();
			SubscribableChannel moduleOutputChannel = new DirectChannel();
			String testTopicName = "existing" + System.currentTimeMillis();
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			configurationProperties.setAutoAddPartitions(true);
			Binder binder = getBinder(configurationProperties);
			ConfigurableApplicationContext context = TestUtils.getPropertyValue(binder, "binder.applicationContext",
					ConfigurableApplicationContext.class);
			MessagingMessageConverter converter = new MessagingMessageConverter();
			converter.setGenerateMessageId(true);
			converter.setGenerateTimestamp(true);
			context.getBeanFactory().registerSingleton("testConverter", converter);
			QueueChannel moduleInputChannel = new QueueChannel();
			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
			producerProperties.setUseNativeEncoding(true);
			producerProperties.getExtension()
					.getConfiguration()
					.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
			producerBinding = binder.bindProducer(testTopicName, moduleOutputChannel, producerProperties);
			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);
			consumerProperties.getExtension()
					.getConfiguration()
					.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
			consumerProperties.getExtension().setConverterBeanName("testConverter");
			consumerBinding = binder.bindConsumer(testTopicName, "test", moduleInputChannel, consumerProperties);
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			Message<?> inbound = receive(moduleInputChannel, 500);
			assertThat(inbound).isNotNull();
			assertThat(inbound.getPayload()).isEqualTo(new byte[1]);
			assertThat(inbound.getHeaders()).containsKey("contentType");
			assertThat(inbound.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString()).isEqualTo("something/funky");
			assertThat(inbound.getHeaders().getId()).isNotNull();
			assertThat(inbound.getHeaders().getTimestamp()).isNotNull();
		}
		finally {
			if (producerBinding != null) {
				producerBinding.unbind();
			}
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testBuiltinSerialization() throws Exception {
		Binding<?> producerBinding = null;
		Binding<?> consumerBinding = null;
		try {
			String testPayload = "test";
			Message<?> message = MessageBuilder.withPayload(testPayload)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN)
					.build();

			ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();

			DirectChannel moduleOutputChannel = createBindableChannel("output",
					createProducerBindingProperties(producerProperties));

			ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoRebalanceEnabled(false);

			DirectChannel moduleInputChannel = createBindableChannel("input", createConsumerBindingProperties(consumerProperties));

			String testTopicName = "existing" + System.currentTimeMillis();
			KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
			configurationProperties.setAutoAddPartitions(true);
			Binder binder = getBinder(configurationProperties);
			producerBinding = binder.bindProducer(testTopicName, moduleOutputChannel, producerProperties);

			consumerBinding = binder.bindConsumer(testTopicName, "test", moduleInputChannel, consumerProperties);
			// Let the consumer actually bind to the producer before sending a msg
			binderBindUnbindLatency();
			moduleOutputChannel.send(message);
			CountDownLatch latch = new CountDownLatch(1);
			AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
			moduleInputChannel.subscribe(message1 -> {
				try {
					inboundMessageRef.set((Message<byte[]>) message1);
				}
				finally {
					latch.countDown();
				}
			});
			Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

			assertThat(inboundMessageRef.get()).isNotNull();
			assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo("test");
			assertThat(inboundMessageRef.get().getHeaders()).containsEntry("contentType", MimeTypeUtils.TEXT_PLAIN);
		}
		finally {
			if (producerBinding != null) {
				producerBinding.unbind();
			}
			if (consumerBinding != null) {
				consumerBinding.unbind();
			}
		}
	}

	/*
	 * Verify that a consumer configured to handle embedded headers can handle
	 * all three variants.
	 */
	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testSendAndReceiveWithMixedMode() throws Exception {
		KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
		binderConfiguration.setHeaders("foo");
		Binder binder = getBinder(binderConfiguration);
		DirectChannel moduleOutputChannel1 = new DirectChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties1 = createProducerProperties();
		producerProperties1.setHeaderMode(HeaderMode.embeddedHeaders);
		Binding<MessageChannel> producerBinding1 = binder.bindProducer("mixed.0", moduleOutputChannel1,
				producerProperties1);

		DirectChannel moduleOutputChannel2 = new DirectChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties2 = createProducerProperties();
		producerProperties2.setHeaderMode(HeaderMode.headers);
		Binding<MessageChannel> producerBinding2 = binder.bindProducer("mixed.0", moduleOutputChannel2,
				producerProperties2);

		DirectChannel moduleOutputChannel3 = new DirectChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties3 = createProducerProperties();
		producerProperties3.setHeaderMode(HeaderMode.none);
		Binding<MessageChannel> producerBinding3 = binder.bindProducer("mixed.0", moduleOutputChannel3,
				producerProperties3);

		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setHeaderMode(HeaderMode.embeddedHeaders);
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));
		QueueChannel bridged = new QueueChannel();
		BridgeHandler bridge = new BridgeHandler();
		bridge.setOutputChannel(bridged);
		moduleInputChannel.subscribe(bridge);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("mixed.0", "test", moduleInputChannel,
				consumerProperties);
		Message<?> message = org.springframework.integration.support.MessageBuilder
				.withPayload("testSendAndReceiveWithMixedMode".getBytes())
				.setHeader("foo", "bar")
				.build();
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel1.send(message);
		moduleOutputChannel2.send(message);
		moduleOutputChannel3.send(message);
		Message<?> inbound = receive(bridged, 10_000);
		assertThat(inbound).isNotNull();
		assertThat(new String((byte[])inbound.getPayload(), StandardCharsets.UTF_8)).isEqualTo("testSendAndReceiveWithMixedMode");
		assertThat(inbound.getHeaders().get("foo")).isEqualTo("bar");
		assertThat(inbound.getHeaders().get(BinderHeaders.NATIVE_HEADERS_PRESENT)).isNull();
		inbound = receive(bridged);
		assertThat(inbound).isNotNull();
		assertThat(new String((byte[])inbound.getPayload(), StandardCharsets.UTF_8)).isEqualTo("testSendAndReceiveWithMixedMode");
		assertThat(inbound.getHeaders().get("foo")).isEqualTo("bar");
		assertThat(inbound.getHeaders().get(BinderHeaders.NATIVE_HEADERS_PRESENT)).isEqualTo(Boolean.TRUE);
		inbound = receive(bridged);
		assertThat(inbound).isNotNull();
		assertThat(new String((byte[])inbound.getPayload(), StandardCharsets.UTF_8)).isEqualTo("testSendAndReceiveWithMixedMode");
		assertThat(inbound.getHeaders().get("foo")).isNull();
		assertThat(inbound.getHeaders().get(BinderHeaders.NATIVE_HEADERS_PRESENT)).isNull();

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testSendAndReceiveWithMixedMode", "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		DefaultKafkaConsumerFactory cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer consumer = cf.createConsumer();
		consumer.subscribe(Collections.singletonList("mixed.0"));

		ConsumerRecords records = consumer.poll(10_1000);
		Iterator<ConsumerRecord> iterator = records.iterator();
		ConsumerRecord record = iterator.next();
		byte[] value = (byte[]) record.value();
		assertThat(value[0] & 0xff).isEqualTo(0xff);
		assertThat(record.headers().toArray().length).isEqualTo(0);
		record = iterator.next();
		value = (byte[]) record.value();
		assertThat(value[0] & 0xff).isNotEqualTo(0xff);
		assertThat(record.headers().toArray().length).isEqualTo(2);
		record = iterator.next();
		value = (byte[]) record.value();
		assertThat(value[0] & 0xff).isNotEqualTo(0xff);
		assertThat(record.headers().toArray().length).isEqualTo(0);
		consumer.close();

		producerBinding1.unbind();
		producerBinding2.unbind();
		producerBinding3.unbind();
		consumerBinding.unbind();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testPolledConsumer() throws Exception {
		KafkaTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(this.messageConverter);
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer("pollable", "group",
				inboundBindTarget, createConsumerProperties());
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		KafkaTemplate template = new KafkaTemplate(new DefaultKafkaProducerFactory<>(producerProps));
		template.send("pollable", "testPollable");
		boolean polled = inboundBindTarget.poll(m -> {
			assertThat(m.getPayload()).isEqualTo("testPollable");
		});
		int n = 0;
		while (n++ < 100 && !polled) {
			polled = inboundBindTarget.poll(m -> {
				assertThat(m.getPayload()).isEqualTo("testPollable".getBytes());
			});
			Thread.sleep(100);
		}
		assertThat(polled).isTrue();
		binding.unbind();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testPolledConsumerWithDlq() throws Exception {
		KafkaTestBinder binder = getBinder();
		PollableSource<MessageHandler> inboundBindTarget = new DefaultPollableMessageSource(this.messageConverter);
		ExtendedConsumerProperties<KafkaConsumerProperties> properties = createConsumerProperties();
		properties.setMaxAttempts(2);
		properties.setBackOffInitialInterval(0);
		properties.getExtension().setEnableDlq(true);
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		Binding<PollableSource<MessageHandler>> binding = binder.bindPollableConsumer("pollableDlq", "group",
				inboundBindTarget, properties);
		KafkaTemplate template = new KafkaTemplate(new DefaultKafkaProducerFactory<>(producerProps));
		template.send("pollableDlq", "testPollableDLQ");
		try {
			int n = 0;
			while (n++ < 100) {
				inboundBindTarget.poll(m -> {
					throw new RuntimeException("test DLQ");
				});
				Thread.sleep(100);
			}
		}
		catch (MessageHandlingException e) {
			assertThat(e.getCause().getMessage()).isEqualTo("test DLQ");
		}
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dlq", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "error.pollableDlq.group");
		ConsumerRecord deadLetter = KafkaTestUtils.getSingleRecord(consumer, "error.pollableDlq.group");
		assertThat(deadLetter).isNotNull();
		assertThat(deadLetter.value()).isEqualTo("testPollableDLQ");
		binding.unbind();
		consumer.close();
	}

	private final class FailingInvocationCountingMessageHandler implements MessageHandler {

		private int invocationCount;

		private final LinkedHashMap<Long, Message<?>> receivedMessages = new LinkedHashMap<>();

		private final CountDownLatch latch;

		private FailingInvocationCountingMessageHandler(int latchSize) {
			latch = new CountDownLatch(latchSize);
		}

		private FailingInvocationCountingMessageHandler() {
			this(1);
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			invocationCount++;
			Long offset = message.getHeaders().get(KafkaBinderTests.this.getKafkaOffsetHeaderKey(), Long.class);
			// using the offset as key allows to ensure that we don't store duplicate
			// messages on retry
			if (!receivedMessages.containsKey(offset)) {
				receivedMessages.put(offset, message);
				latch.countDown();
			}
			throw new RuntimeException("fail");
		}

		public LinkedHashMap<Long, Message<?>> getReceivedMessages() {
			return receivedMessages;
		}

		public int getInvocationCount() {
			return invocationCount;
		}

		public CountDownLatch getLatch() {
			return latch;
		}
	}
}
