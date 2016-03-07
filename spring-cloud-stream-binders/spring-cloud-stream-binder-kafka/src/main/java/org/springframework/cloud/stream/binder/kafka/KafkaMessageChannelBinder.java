/*
 * Copyright 2014-2016 the original author or authors.
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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import scala.collection.Seq;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.EmbeddedHeadersMessageConverter;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.FixedSubscriberChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.listener.Acknowledgment;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.listener.KafkaNativeOffsetManager;
import org.springframework.integration.kafka.listener.OffsetManager;
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.integration.kafka.support.ProducerConfiguration;
import org.springframework.integration.kafka.support.ProducerFactoryBean;
import org.springframework.integration.kafka.support.ProducerListener;
import org.springframework.integration.kafka.support.ProducerMetadata;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Binder} that uses Kafka as the underlying middleware.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 */
public class KafkaMessageChannelBinder extends AbstractBinder<MessageChannel, KafkaConsumerProperties, KafkaProducerProperties> {

	public static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

	private RetryOperations retryOperations;

	private final Map<String, Collection<Partition>> topicsInUse = new HashMap<>();

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new
			EmbeddedHeadersMessageConverter();

	private final ZookeeperConnect zookeeperConnect;

	private final String brokers;

	private String[] headersToMap;

	private final String zkAddress;

	// -------- Default values for properties -------

	private int replicationFactor = 1;

	private int requiredAcks = 1;

	private int queueSize = 1024;

	private int maxWait = 100;

	private int fetchSize = 1024 * 1024;

	private int defaultMinPartitionCount = 1;

	private ConnectionFactory connectionFactory;

	private int socketBufferSize = 2097152;

	private int offsetUpdateTimeWindow = 10000;

	private int offsetUpdateCount = 0;

	private int offsetUpdateShutdownTimeout = 2000;

	private int zkSessionTimeout = 10000;

	private int zkConnectionTimeout = 10000;

	private ProducerListener producerListener;

	public KafkaMessageChannelBinder(ZookeeperConnect zookeeperConnect, String brokers, String zkAddress,
									 String... headersToMap) {
		this.zookeeperConnect = zookeeperConnect;
		this.brokers = brokers;
		this.zkAddress = zkAddress;
		if (headersToMap.length > 0) {
			String[] combinedHeadersToMap =
					Arrays.copyOfRange(BinderHeaders.STANDARD_HEADERS, 0, BinderHeaders.STANDARD_HEADERS.length + headersToMap
							.length);
			System.arraycopy(headersToMap, 0, combinedHeadersToMap, BinderHeaders.STANDARD_HEADERS.length, headersToMap
					.length);
			this.headersToMap = combinedHeadersToMap;
		}
		else {
			this.headersToMap = BinderHeaders.STANDARD_HEADERS;
		}
	}

	String getZkAddress() {
		return this.zkAddress;
	}

	public void setSocketBufferSize(int socketBufferSize) {
		this.socketBufferSize = socketBufferSize;
	}

	public void setOffsetUpdateTimeWindow(int offsetUpdateTimeWindow) {
		this.offsetUpdateTimeWindow = offsetUpdateTimeWindow;
	}

	public void setOffsetUpdateCount(int offsetUpdateCount) {
		this.offsetUpdateCount = offsetUpdateCount;
	}

	public void setOffsetUpdateShutdownTimeout(int offsetUpdateShutdownTimeout) {
		this.offsetUpdateShutdownTimeout = offsetUpdateShutdownTimeout;
	}

	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setProducerListener(ProducerListener producerListener) {
		this.producerListener = producerListener;
	}

	/**
	 * Retry configuration for operations such as validating topic creation
	 * @param retryOperations the retry configuration
	 */
	public void setRetryOperations(RetryOperations retryOperations) {
		this.retryOperations = retryOperations;
	}

	@Override
	public void onInit() throws Exception {
		ZookeeperConfiguration configuration = new ZookeeperConfiguration(this.zookeeperConnect);
		configuration.setBufferSize(socketBufferSize);
		configuration.setMaxWait(maxWait);
		DefaultConnectionFactory defaultConnectionFactory =
				new DefaultConnectionFactory(configuration);
		defaultConnectionFactory.afterPropertiesSet();
		this.connectionFactory = defaultConnectionFactory;
		if (retryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(10);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(100);
			backOffPolicy.setMultiplier((double) 2);
			backOffPolicy.setMaxInterval(1000);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			retryOperations = retryTemplate;
		}
	}

	/**
	 * Allowed chars are ASCII alphanumerics, '.', '_' and '-'.
	 */
	public static void validateTopicName(String topicName) {
		try {
			byte[] utf8 = topicName.getBytes("UTF-8");
			for (byte b : utf8) {
				if (!((b >= 'a') && (b <= 'z') || (b >= 'A') && (b <= 'Z') || (b >= '0') && (b <= '9') || (b == '.')
						|| (b == '-') || (b == '_'))) {
					throw new IllegalArgumentException("Topic name can only have ASCII alphanumerics, '.', '_' and '-'");
				}
			}
		}
		catch (UnsupportedEncodingException e) {
			throw new AssertionError(e); // Can't happen
		}
	}

	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public void setRequiredAcks(int requiredAcks) {
		this.requiredAcks = requiredAcks;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public void setDefaultMinPartitionCount(int defaultMinPartitionCount) {
		this.defaultMinPartitionCount = defaultMinPartitionCount;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getZkSessionTimeout() {
		return this.zkSessionTimeout;
	}

	public void setZkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
	}

	public int getZkConnectionTimeout() {
		return this.zkConnectionTimeout;
	}

	public void setZkConnectionTimeout(int zkConnectionTimeout) {
		this.zkConnectionTimeout = zkConnectionTimeout;
	}

	Map<String, Collection<Partition>> getTopicsInUse() {
		return this.topicsInUse;
	}

	@Override
	protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel, KafkaConsumerProperties properties) {
		// If the caller provides a consumer group, use it; otherwise an anonymous consumer group
		// is generated each time, such that each anonymous binding will receive all messages.
		// Consumers reset offsets at the latest time by default, which allows them to receive only
		// messages sent after they've been bound. That behavior can be changed with the
		// "resetOffsets" and "startOffset" properties.
		boolean anonymous = !StringUtils.hasText(group);
		String consumerGroup = anonymous ? "anonymous." + UUID.randomUUID().toString() : group;
		// The reference point, if not set explicitly is the latest time for anonymous subscriptions and the
		// earliest time for group subscriptions. This allows the latter to receive messages published before the group
		// has been created.
		long referencePoint = properties.getStartOffset() != null ?
				properties.getStartOffset().getReferencePoint() : (anonymous ? OffsetRequest.LatestTime() : OffsetRequest.EarliestTime());
		return createKafkaConsumer(name, inputChannel, properties, consumerGroup, referencePoint);
	}


	@Override
	public Binding<MessageChannel> doBindProducer(String name, MessageChannel moduleOutputChannel, KafkaProducerProperties properties) {

		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		if (logger.isInfoEnabled()) {
			logger.info("Using kafka topic for outbound: " + name);
		}

		validateTopicName(name);

		int numPartitions = Math.max(defaultMinPartitionCount, properties.getPartitionCount());
		Collection<Partition> partitions = ensureTopicCreated(name, numPartitions, replicationFactor);

		topicsInUse.put(name, partitions);

		ProducerMetadata<byte[], byte[]> producerMetadata = new ProducerMetadata<>(name, byte[].class, byte[].class,
				BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
		producerMetadata.setSync(properties.isSync());
		producerMetadata.setCompressionType(properties.getCompressionType());
		producerMetadata.setBatchBytes(properties.getBufferSize());
		Properties additionalProps = new Properties();
		additionalProps.put(ProducerConfig.ACKS_CONFIG, String.valueOf(requiredAcks));
		additionalProps.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(properties.getBatchTimeout()));
		ProducerFactoryBean<byte[], byte[]> producerFB = new ProducerFactoryBean<>(producerMetadata, brokers, additionalProps);

		try {
			final ProducerConfiguration<byte[], byte[]> producerConfiguration = new ProducerConfiguration<>(
					producerMetadata, producerFB.getObject());
			producerConfiguration.setProducerListener(producerListener);

			MessageHandler handler = new SendingHandler(name, properties, partitions.size(), producerConfiguration);
			EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel,
					handler);
			consumer.setBeanFactory(this.getBeanFactory());
			consumer.setBeanName("outbound." + name);
			consumer.afterPropertiesSet();
			DefaultBinding<MessageChannel> producerBinding = new DefaultBinding<>(name, null, moduleOutputChannel, consumer);
			consumer.start();
			return producerBinding;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates a Kafka topic if needed, or try to increase its partition count to the desired number.
	 */
	private Collection<Partition> ensureTopicCreated(final String topicName, final int numPartitions,
			int replicationFactor) {

		final ZkClient zkClient = new ZkClient(zkAddress, getZkSessionTimeout(), getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);
		try {
			// The following is basically copy/paste from AdminUtils.createTopic() with
			// createOrUpdateTopicPartitionAssignmentPathInZK(..., update=true)
			final Properties topicConfig = new Properties();
			Seq<Object> brokerList = ZkUtils.getSortedBrokerList(zkClient);
			final scala.collection.Map<Object, Seq<Object>> replicaAssignment = AdminUtils.assignReplicasToBrokers
					(brokerList,
							numPartitions, replicationFactor, -1, -1);
			retryOperations.execute(new RetryCallback<Object, RuntimeException>() {

				@Override
				public Object doWithRetry(RetryContext context) throws RuntimeException {
					AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topicName, replicaAssignment,
							topicConfig, true);
					return null;
				}
			});
			try {
				Collection<Partition> partitions = retryOperations.execute(new RetryCallback<Collection<Partition>, Exception>() {

					@Override
					public Collection<Partition> doWithRetry(RetryContext context) throws Exception {
						connectionFactory.refreshMetadata(Collections.singleton(topicName));
						Collection<Partition> partitions = connectionFactory.getPartitions(topicName);
						if (partitions.size() < numPartitions) {
							throw new IllegalStateException("The number of expected partitions was: " + numPartitions
									+ ", but " +
									partitions.size() + " have been found instead");
						}
						connectionFactory.getLeaders(partitions);
						return partitions;
					}
				});
				return partitions;
			}
			catch (Exception e) {
				logger.error("Cannot initialize Binder", e);
				throw new BinderException("Cannot initialize binder:", e);
			}

		}
		finally {
			zkClient.close();
		}
	}

	private Binding<MessageChannel> createKafkaConsumer(String name, final MessageChannel moduleInputChannel,
														KafkaConsumerProperties properties, String group, long referencePoint) {

		validateTopicName(name);
		int minKafkaPartitions = properties.getMinPartitionCount();
		int instance = properties.getInstanceCount();
		if (instance == 0) {
			throw new IllegalArgumentException("Instance count cannot be zero");
		}
		int numPartitions = Math.max(minKafkaPartitions, instance * properties.getConcurrency());
		Collection<Partition> allPartitions = ensureTopicCreated(name, numPartitions, replicationFactor);

		Decoder<byte[]> valueDecoder = new DefaultDecoder(null);
		Decoder<byte[]> keyDecoder = new DefaultDecoder(null);

		Collection<Partition> listenedPartitions;

		if (instance == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<>();
			for (Partition partition : allPartitions) {
				// divide partitions across modules
				if ((partition.getId() % instance) == properties.getInstanceIndex()) {
					listenedPartitions.add(partition);
				}
			}
		}
		topicsInUse.put(name, listenedPartitions);
		ReceivingHandler rh = new ReceivingHandler(properties);
		rh.setOutputChannel(moduleInputChannel);

		final FixedSubscriberChannel bridge = new FixedSubscriberChannel(rh);
		bridge.setBeanName("bridge." + name);

		final KafkaMessageListenerContainer messageListenerContainer =
				createMessageListenerContainer(properties, group, null, listenedPartitions, referencePoint);

		final KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter(messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		kafkaMessageDrivenChannelAdapter.setKeyDecoder(keyDecoder);
		kafkaMessageDrivenChannelAdapter.setPayloadDecoder(valueDecoder);
		kafkaMessageDrivenChannelAdapter.setOutputChannel(bridge);
		kafkaMessageDrivenChannelAdapter.setAutoCommitOffset(properties.isAutoCommitOffset());
		kafkaMessageDrivenChannelAdapter.afterPropertiesSet();
		kafkaMessageDrivenChannelAdapter.start();


		EventDrivenConsumer edc = new EventDrivenConsumer(bridge, rh) {

			@Override
			protected void doStop() {
				// stop the offset manager and the channel adapter before unbinding
				// this means that the upstream channel adapter has a chance to stop
				kafkaMessageDrivenChannelAdapter.stop();
				if (messageListenerContainer.getOffsetManager() instanceof DisposableBean) {
					try {
						((DisposableBean) messageListenerContainer.getOffsetManager()).destroy();
					}
					catch (Exception e) {
						logger.error("Error while closing the offset manager", e);
					}
				}
				super.doStop();
			}
		};
		String groupedName = groupedName(name, group);
		edc.setBeanName("inbound." + groupedName);

		DefaultBinding<MessageChannel> consumerBinding = new DefaultBinding<>(name, group, moduleInputChannel, edc);
		edc.start();
		return consumerBinding;
	}

	KafkaMessageListenerContainer createMessageListenerContainer(KafkaConsumerProperties consumerProperties,
																 String group, String topic, Collection<Partition> listenedPartitions,
																 long referencePoint) {
		Assert.isTrue(StringUtils.hasText(topic) ^ !CollectionUtils.isEmpty(listenedPartitions),
				"Exactly one of topic or a list of listened partitions must be provided");
		KafkaMessageListenerContainer messageListenerContainer;
		if (topic != null) {
			messageListenerContainer = new KafkaMessageListenerContainer(connectionFactory, topic);
		}
		else {
			messageListenerContainer = new KafkaMessageListenerContainer(connectionFactory,
					listenedPartitions.toArray(new Partition[listenedPartitions.size()]));
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Listening to topic " + topic);
		}
		// if we have fewer target partitions than target concurrency, adjust accordingly
		messageListenerContainer.setConcurrency(Math.min(consumerProperties.getConcurrency(), listenedPartitions.size()));
		OffsetManager offsetManager = createOffsetManager(group, referencePoint);
		if (consumerProperties.isResetOffsets()) {
			offsetManager.resetOffsets(listenedPartitions);
		}
		messageListenerContainer.setOffsetManager(offsetManager);
		messageListenerContainer.setQueueSize(queueSize);
		messageListenerContainer.setMaxFetch(fetchSize);
		return messageListenerContainer;
	}

	private OffsetManager createOffsetManager(String group, long referencePoint) {
		try {

			KafkaNativeOffsetManager kafkaOffsetManager =
					new KafkaNativeOffsetManager(connectionFactory, zookeeperConnect,
							Collections.<Partition, Long>emptyMap());
			kafkaOffsetManager.setConsumerId(group);
			kafkaOffsetManager.setReferenceTimestamp(referencePoint);
			kafkaOffsetManager.afterPropertiesSet();

			WindowingOffsetManager windowingOffsetManager = new WindowingOffsetManager(kafkaOffsetManager);
			windowingOffsetManager.setTimespan(offsetUpdateTimeWindow);
			windowingOffsetManager.setCount(offsetUpdateCount);
			windowingOffsetManager.setShutdownTimeout(offsetUpdateShutdownTimeout);

			windowingOffsetManager.afterPropertiesSet();
			return windowingOffsetManager;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void doManualAck(LinkedList<MessageHeaders> messageHeadersList) {
		Iterator<MessageHeaders> iterator = messageHeadersList.iterator();
		while (iterator.hasNext()) {
			MessageHeaders headers = iterator.next();
			Acknowledgment acknowledgment = (Acknowledgment) headers.get(KafkaHeaders.ACKNOWLEDGMENT);
			Assert.notNull(acknowledgment, "Acknowledgement shouldn't be null when acknowledging kafka message " +
					"manually.");
			acknowledgment.acknowledge();
		}
	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		private KafkaConsumerProperties consumerProperties;

		public ReceivingHandler(KafkaConsumerProperties consumerProperties) {
			this.consumerProperties = consumerProperties;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Object handleRequestMessage(Message<?> requestMessage) {
			if (Mode.embeddedHeaders.equals(consumerProperties.getMode())) {
				MessageValues messageValues;
				try {
					messageValues = embeddedHeadersMessageConverter.extractHeaders((Message<byte[]>) requestMessage,
							true);
				}
				catch (Exception e) {
					logger.error(EmbeddedHeadersMessageConverter.decodeExceptionMessage(requestMessage), e);
					messageValues = new MessageValues(requestMessage);
				}
				messageValues = deserializePayloadIfNecessary(messageValues);
				return MessageBuilder.createMessage(messageValues.getPayload(), new KafkaBinderHeaders(
						messageValues));
			}
			else {
				return requestMessage;
			}
		}

		@SuppressWarnings("serial")
		private final class KafkaBinderHeaders extends MessageHeaders {

			KafkaBinderHeaders(Map<String, Object> headers) {
				super(headers, MessageHeaders.ID_VALUE_NONE, -1L);
			}
		}


		@Override
		protected boolean shouldCopyRequestHeaders() {
			// prevent the message from being copied again in superclass
			return false;
		}
	}

	private class SendingHandler extends AbstractMessageHandler {

		private final AtomicInteger roundRobinCount = new AtomicInteger();

		private final String topicName;

		private final KafkaProducerProperties producerProperties;

		private final int numberOfKafkaPartitions;

		private final ProducerConfiguration<byte[], byte[]> producerConfiguration;

		private final PartitionHandler partitionHandler;

		private SendingHandler(String topicName, KafkaProducerProperties properties, int numberOfPartitions,
							   ProducerConfiguration<byte[], byte[]> producerConfiguration) {
			this.topicName = topicName;
			producerProperties = properties;
			this.numberOfKafkaPartitions = numberOfPartitions;
			ConfigurableListableBeanFactory beanFactory = KafkaMessageChannelBinder.this.getBeanFactory();
			this.setBeanFactory(beanFactory);
			this.producerConfiguration = producerConfiguration;
			this.partitionHandler = new PartitionHandler(beanFactory, evaluationContext, partitionSelector,
					properties);
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			int targetPartition;
			if (producerProperties.isPartitioned()) {
				targetPartition = this.partitionHandler.determinePartition(message);
			}
			else {
				targetPartition = roundRobin() % numberOfKafkaPartitions;
			}

			if (Mode.embeddedHeaders.equals(producerProperties.getMode())) {
				MessageValues transformed = serializePayloadIfNecessary(message);
				byte[] messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
						KafkaMessageChannelBinder.this.headersToMap);
				producerConfiguration.send(topicName, targetPartition, null, messageToSend);
			}
			else if (Mode.raw.equals(producerProperties.getMode())) {
				Object contentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
				if (contentType != null
						&& !contentType.equals(MediaType.APPLICATION_OCTET_STREAM_VALUE)) {
					logger.error("Raw mode supports only " + MediaType.APPLICATION_OCTET_STREAM_VALUE + " content type"
							+ message.getPayload().getClass());
				}
				if (message.getPayload() instanceof byte[]) {
					producerConfiguration.send(topicName, targetPartition, null, (byte[]) message.getPayload());
				}
				else {
					logger.error("Raw mode supports only byte[] payloads but value sent was of type "
							+ message.getPayload().getClass());
				}
			}
		}

		private int roundRobin() {
			int result = roundRobinCount.incrementAndGet();
			if (result == Integer.MAX_VALUE) {
				roundRobinCount.set(0);
			}
			return result;
		}

	}

	public enum Mode {
		raw,
		embeddedHeaders
	}

	public enum StartOffset {
		earliest(OffsetRequest.EarliestTime()),
		latest(OffsetRequest.LatestTime());

		private final long referencePoint;

		StartOffset(long referencePoint) {
			this.referencePoint = referencePoint;
		}

		public long getReferencePoint() {
			return referencePoint;
		}
	}

}
