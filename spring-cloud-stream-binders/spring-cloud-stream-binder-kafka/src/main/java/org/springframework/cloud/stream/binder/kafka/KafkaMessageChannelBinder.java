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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.DefaultBindingPropertiesAccessor;
import org.springframework.cloud.stream.binder.EmbeddedHeadersMessageConverter;
import org.springframework.cloud.stream.binder.MessageValues;
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

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.Seq;

/**
 * A binder that uses Kafka as the underlying middleware. The general implementation mapping between XD concepts
 * and Kafka concepts is as follows:
 * A binder that uses Kafka as the underlying middleware.
 * The general implementation mapping between XD concepts and Kafka concepts is as follows:
 * <table>
 * <tr>
 * <th>Stream definition</th><th>Kafka topic</th><th>Kafka partitions</th><th>Notes</th>
 * </tr>
 * <tr>
 * <td>foo = "http | log"</td><td>foo.0</td><td>1 partition</td><td>1 producer, 1 consumer</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=x</td><td>foo.0</td><td>x partitions</td><td>1 producer, x consumers with static
 * group 'springXD', achieves queue semantics</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=x + XD partitioning</td><td>still 1 topic 'foo.0'</td><td>x partitions + use key
 * computed by XD</td><td>1 producer, x consumers with static group 'springXD', achieves queue semantics</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=x, concurrency=y</td><td>foo.0</td><td>x*y partitions</td><td>1 producer, x XD
 * consumers, each with y threads</td>
 * </tr>
 * <tr>
 * <td>foo = "http | log", log.count=0, x actual log containers</td><td>foo.0</td><td>10(configurable)
 * partitions</td><td>1 producer, x XD consumers. Can't know the number of partitions beforehand, so decide a number
 * that better be greater than number of containers</td>
 * </tr>
 * </table>
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Gary Russell
 * @author Mark Fisher
 * @author Soby Chacko
 */
public class KafkaMessageChannelBinder extends AbstractBinder<MessageChannel> {

	public static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

	public static final int METADATA_VERIFICATION_RETRY_ATTEMPTS = 10;

	public static final double METADATA_VERIFICATION_RETRY_BACKOFF_MULTIPLIER = 2;

	public static final int METADATA_VERIFICATION_RETRY_INITIAL_INTERVAL = 100;

	public static final int METADATA_VERIFICATION_MAX_INTERVAL = 1000;

	public static final String FETCH_SIZE = "fetchSize";

	public static final String QUEUE_SIZE = "fetchSize";

	public static final String REQUIRED_ACKS = "requiredAcks";

	public static final String COMPRESSION_CODEC = "compressionCodec";

	public static final String AUTO_COMMIT_ENABLED = "autoCommitEnabled";

	private static final String DEFAULT_COMPRESSION_CODEC = "none";

	private static final int DEFAULT_REQUIRED_ACKS = 1;

	private static final boolean DEFAULT_AUTO_COMMIT_ENABLED = true;

	private static final boolean DEFAULT_RESET_OFFSETS = false;

	private static final int DEFAULT_ZK_SESSION_TIMEOUT = 10000;

	private static final int DEFAULT_ZK_CONNECTION_TIMEOUT = 10000;

	private static final StartOffset DEFAULT_START_OFFSET = StartOffset.latest;

	private RetryOperations retryOperations;

	private Map<String, Collection<Partition>> topicsInUse = new HashMap<>();

	protected static final Set<Object> PRODUCER_COMPRESSION_PROPERTIES = new HashSet<Object>(
			Arrays.asList(new String[] {
				KafkaMessageChannelBinder.COMPRESSION_CODEC,
			}));

	private static final Set<Object> KAFKA_CONSUMER_PROPERTIES = new SetBuilder()
			.add(BinderPropertyKeys.MIN_PARTITION_COUNT)
			.build();

	/**
	 * Basic + concurrency + partitioning.
	 */
	private static final Set<Object> SUPPORTED_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.addAll(KAFKA_CONSUMER_PROPERTIES)
			.add(BinderPropertyKeys.PARTITION_INDEX) // Not actually used
			.add(BinderPropertyKeys.COUNT) // Not actually used
			.add(BinderPropertyKeys.CONCURRENCY)
			.add(FETCH_SIZE)
			.build();

	private static final Set<Object> KAFKA_PRODUCER_PROPERTIES = new SetBuilder()
			.add(BinderPropertyKeys.MIN_PARTITION_COUNT)
			.build();

	/**
	 * Partitioning + kafka producer properties.
	 */
	private static final Set<Object> SUPPORTED_PRODUCER_PROPERTIES = new SetBuilder()
			.addAll(PRODUCER_PARTITIONING_PROPERTIES)
			.addAll(PRODUCER_STANDARD_PROPERTIES)
			.addAll(KAFKA_PRODUCER_PROPERTIES)
			.addAll(PRODUCER_BATCHING_BASIC_PROPERTIES)
			.addAll(PRODUCER_COMPRESSION_PROPERTIES)
			.build();

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new
			EmbeddedHeadersMessageConverter();

	private final ZookeeperConnect zookeeperConnect;

	private final String brokers;

	private String[] headersToMap;

	private final String zkAddress;

	// -------- Default values for properties -------

	private int defaultReplicationFactor = 1;

	private String defaultCompressionCodec = DEFAULT_COMPRESSION_CODEC;

	private int defaultRequiredAcks = DEFAULT_REQUIRED_ACKS;

	private int defaultQueueSize = 1024;

	private int defaultMaxWait = 100;

	private int defaultFetchSize = 1024 * 1024;

	private int defaultMinPartitionCount = 1;

	private ConnectionFactory connectionFactory;

	// auto commit property

	private boolean defaultAutoCommitEnabled = DEFAULT_AUTO_COMMIT_ENABLED;

	private int socketBufferSize = 2097152;

	private int offsetUpdateTimeWindow = 10000;

	private int offsetUpdateCount = 0;

	private int offsetUpdateShutdownTimeout = 2000;

	private Mode mode = Mode.embeddedHeaders;

	private boolean resetOffsets = DEFAULT_RESET_OFFSETS;

	private StartOffset startOffset = DEFAULT_START_OFFSET;

	private int zkSessionTimeout = DEFAULT_ZK_SESSION_TIMEOUT;

	private int zkConnectionTimeout = DEFAULT_ZK_CONNECTION_TIMEOUT;

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
	public void afterPropertiesSet() throws Exception {
		// we instantiate the connection factory here due to https://jira.spring.io/browse/XD-2647
		ZookeeperConfiguration configuration = new ZookeeperConfiguration(this.zookeeperConnect);
		configuration.setBufferSize(socketBufferSize);
		configuration.setMaxWait(defaultMaxWait);
		DefaultConnectionFactory defaultConnectionFactory =
				new DefaultConnectionFactory(configuration);
		defaultConnectionFactory.afterPropertiesSet();
		this.connectionFactory = defaultConnectionFactory;
		if (retryOperations == null) {
			RetryTemplate retryTemplate = new RetryTemplate();

			SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
			simpleRetryPolicy.setMaxAttempts(METADATA_VERIFICATION_RETRY_ATTEMPTS);
			retryTemplate.setRetryPolicy(simpleRetryPolicy);

			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(METADATA_VERIFICATION_RETRY_INITIAL_INTERVAL);
			backOffPolicy.setMultiplier(METADATA_VERIFICATION_RETRY_BACKOFF_MULTIPLIER);
			backOffPolicy.setMaxInterval(METADATA_VERIFICATION_MAX_INTERVAL);
			retryTemplate.setBackOffPolicy(backOffPolicy);
			retryOperations = retryTemplate;
		}
		super.afterPropertiesSet();
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

	public void setDefaultReplicationFactor(int defaultReplicationFactor) {
		this.defaultReplicationFactor = defaultReplicationFactor;
	}

	public void setDefaultCompressionCodec(String defaultCompressionCodec) {
		this.defaultCompressionCodec = defaultCompressionCodec;
	}

	public void setDefaultRequiredAcks(int defaultRequiredAcks) {
		this.defaultRequiredAcks = defaultRequiredAcks;
	}

	/**
	 * Set the default auto commit enabled property; This is used to commit the offset either automatically or
	 * manually.
	 * @param defaultAutoCommitEnabled
	 */
	public void setDefaultAutoCommitEnabled(boolean defaultAutoCommitEnabled) {
		this.defaultAutoCommitEnabled = defaultAutoCommitEnabled;
	}

	public void setDefaultQueueSize(int defaultQueueSize) {
		this.defaultQueueSize = defaultQueueSize;
	}

	public void setDefaultFetchSize(int defaultFetchSize) {
		this.defaultFetchSize = defaultFetchSize;
	}

	public void setDefaultMinPartitionCount(int defaultMinPartitionCount) {
		this.defaultMinPartitionCount = defaultMinPartitionCount;
	}

	public void setDefaultMaxWait(int defaultMaxWait) {
		this.defaultMaxWait = defaultMaxWait;
	}

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public boolean isResetOffsets() {
		return resetOffsets;
	}

	public void setResetOffsets(boolean resetOffsets) {
		this.resetOffsets = resetOffsets;
	}

	public StartOffset getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(StartOffset startOffset) {
		this.startOffset = startOffset;
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
	protected Binding<MessageChannel> doBindConsumer(String name, String group, MessageChannel inputChannel, Properties properties) {
		// If the caller provides a consumer group, use it; otherwise an anonymous consumer group
		// is generated each time, such that each anonymous binding will receive all messages.
		// Consumers reset offsets at the latest time by default, which allows them to receive only
		// messages sent after they've been bound. That behavior can be changed with the
		// "resetOffsets" and "startOffset" properties.
		String consumerGroup = group == null ? "anonymous." + UUID.randomUUID().toString() : group;
		long referencePoint = this.startOffset != null ?
				startOffset.getReferencePoint() : OffsetRequest.LatestTime();
		return createKafkaConsumer(name, inputChannel, properties, consumerGroup, referencePoint);
	}

	@Override
	public Binding<MessageChannel> bindProducer(final String name, MessageChannel moduleOutputChannel, Properties properties) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		KafkaPropertiesAccessor producerPropertiesAccessor = new KafkaPropertiesAccessor(properties);
		validateProducerProperties(name, properties, SUPPORTED_PRODUCER_PROPERTIES);
		if (logger.isInfoEnabled()) {
			logger.info("Using kafka topic for outbound: " + name);
		}

		validateTopicName(name);

		int numPartitions = producerPropertiesAccessor.getNumberOfKafkaPartitionsForProducer();

		Collection<Partition> partitions = ensureTopicCreated(name, numPartitions, defaultReplicationFactor);

		topicsInUse.put(name, partitions);

		ProducerMetadata<byte[], byte[]> producerMetadata = new ProducerMetadata<>(
				name, byte[].class, byte[].class, BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
		producerMetadata.setCompressionType(ProducerMetadata.CompressionType.valueOf(
				producerPropertiesAccessor.getCompressionCodec(this.defaultCompressionCodec)));
		producerMetadata.setBatchBytes(producerPropertiesAccessor.getBatchSize(this.defaultBatchSize));
		Properties additionalProps = new Properties();
		additionalProps.put(ProducerConfig.ACKS_CONFIG,
				String.valueOf(producerPropertiesAccessor.getRequiredAcks(this
						.defaultRequiredAcks)));
		additionalProps.put(ProducerConfig.LINGER_MS_CONFIG,
				String.valueOf(producerPropertiesAccessor.getBatchTimeout(this
						.defaultBatchTimeout)));
		ProducerFactoryBean<byte[], byte[]> producerFB =
				new ProducerFactoryBean<>(producerMetadata, brokers, additionalProps);

		try {
			final ProducerConfiguration<byte[], byte[]> producerConfiguration = new ProducerConfiguration<>(
					producerMetadata, producerFB.getObject());
			producerConfiguration.setProducerListener(producerListener);

			MessageHandler handler = new SendingHandler(name, producerPropertiesAccessor,
					partitions.size(), producerConfiguration);
			EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel,
					handler);
			consumer.setBeanFactory(this.getBeanFactory());
			consumer.setBeanName("outbound." + name);
			consumer.afterPropertiesSet();
			DefaultBinding<MessageChannel> producerBinding = new DefaultBinding<>(name, null, moduleOutputChannel, consumer, Binding.Type.producer, producerPropertiesAccessor);
			addBinding(producerBinding);
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

	private Binding<MessageChannel> createKafkaConsumer(String name, final MessageChannel moduleInputChannel, Properties properties,
			String group, long referencePoint) {

		validateConsumerProperties(groupedName(name, group), properties, SUPPORTED_CONSUMER_PROPERTIES);
		KafkaPropertiesAccessor accessor = new KafkaPropertiesAccessor(properties);

		int maxConcurrency = accessor.getConcurrency(defaultConcurrency);

		validateTopicName(name);

		int numPartitions = accessor.getNumberOfKafkaPartitionsForConsumer();
		Collection<Partition> allPartitions = ensureTopicCreated(name, numPartitions, defaultReplicationFactor);

		Decoder<byte[]> valueDecoder = new DefaultDecoder(null);
		Decoder<byte[]> keyDecoder = new DefaultDecoder(null);

		Collection<Partition> listenedPartitions;

		int moduleCount = accessor.getCount();

		if (moduleCount == 1) {
			listenedPartitions = allPartitions;
		}
		else {
			listenedPartitions = new ArrayList<Partition>();
			for (Partition partition : allPartitions) {
				// divide partitions across modules
				if (accessor.getPartitionIndex() != -1) {
					if ((partition.getId() % moduleCount) == accessor.getPartitionIndex()) {
						listenedPartitions.add(partition);
					}
				}
				else {
					int moduleSequence = accessor.getSequence();
					if (moduleCount == 0) {
						throw new IllegalArgumentException("The Kafka transport does not support 0-count modules");
					}
					else {
						// sequence numbers are zero-based
						if ((partition.getId() % moduleCount) == (moduleSequence - 1)) {
							listenedPartitions.add(partition);
						}
					}
				}
			}
		}
		topicsInUse.put(name, listenedPartitions);
		ReceivingHandler rh = new ReceivingHandler();
		rh.setOutputChannel(moduleInputChannel);

		final FixedSubscriberChannel bridge = new FixedSubscriberChannel(rh);
		bridge.setBeanName("bridge." + name);

		final KafkaMessageListenerContainer messageListenerContainer =
				createMessageListenerContainer(accessor, group, maxConcurrency, listenedPartitions,
						referencePoint);

		final KafkaMessageDrivenChannelAdapter kafkaMessageDrivenChannelAdapter =
				new KafkaMessageDrivenChannelAdapter(messageListenerContainer);
		kafkaMessageDrivenChannelAdapter.setBeanFactory(this.getBeanFactory());
		kafkaMessageDrivenChannelAdapter.setKeyDecoder(keyDecoder);
		kafkaMessageDrivenChannelAdapter.setPayloadDecoder(valueDecoder);
		kafkaMessageDrivenChannelAdapter.setOutputChannel(bridge);
		kafkaMessageDrivenChannelAdapter.setAutoCommitOffset(accessor.getDefaultAutoCommitEnabled(this
				.defaultAutoCommitEnabled));
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

		DefaultBinding<MessageChannel> consumerBinding = new DefaultBinding<>(name, group, moduleInputChannel, edc, Binding.Type.consumer, accessor);
		addBinding(consumerBinding);
		edc.start();
		return consumerBinding;
	}

	public KafkaMessageListenerContainer createMessageListenerContainer(Properties properties, String group,
			int maxConcurrency, String topic, long referencePoint) {
		return createMessageListenerContainer(new KafkaPropertiesAccessor(properties), group, maxConcurrency, topic,
				null, referencePoint);
	}

	private KafkaMessageListenerContainer createMessageListenerContainer(KafkaPropertiesAccessor accessor,
			String group, int maxConcurrency, Collection<Partition> listenedPartitions, long referencePoint) {
		return createMessageListenerContainer(accessor, group, maxConcurrency, null, listenedPartitions, referencePoint);
	}

	private KafkaMessageListenerContainer createMessageListenerContainer(KafkaPropertiesAccessor accessor,
			String group, int maxConcurrency, String topic, Collection<Partition> listenedPartitions,
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
		// if we have less target partitions than target concurrency, adjust accordingly
		messageListenerContainer.setConcurrency(Math.min(maxConcurrency, listenedPartitions.size()));
		OffsetManager offsetManager = createOffsetManager(group, referencePoint);
		if (resetOffsets) {
			offsetManager.resetOffsets(listenedPartitions);
		}
		messageListenerContainer.setOffsetManager(offsetManager);
		messageListenerContainer.setQueueSize(accessor.getProperty(QUEUE_SIZE, defaultQueueSize));
		messageListenerContainer.setMaxFetch(accessor.getProperty(FETCH_SIZE, defaultFetchSize));
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

	private class KafkaPropertiesAccessor extends DefaultBindingPropertiesAccessor {

		public KafkaPropertiesAccessor(Properties properties) {
			super(properties);
		}

		public int getNumberOfKafkaPartitionsForProducer() {
			int nextModuleCount = getNextModuleCount();
			if (nextModuleCount == 0) {
				throw new IllegalArgumentException("Module count cannot be zero");
			}
			int nextModuleConcurrency = getProperty(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY, defaultConcurrency);
			int minKafkaPartitions = getMinPartitionCount(defaultMinPartitionCount);
			return Math.max(minKafkaPartitions, nextModuleCount * nextModuleConcurrency);
		}

		public int getNumberOfKafkaPartitionsForConsumer() {
			int concurrency = getConcurrency(defaultConcurrency);
			int minKafkaPartitions = getMinPartitionCount(defaultMinPartitionCount);
			int moduleCount = getCount();
			if (moduleCount == 0) {
				throw new IllegalArgumentException("Module count cannot be zero");
			}
			return Math.max(minKafkaPartitions, moduleCount * concurrency);
		}

		public String getCompressionCodec(String defaultValue) {
			return getProperty(COMPRESSION_CODEC, defaultValue);
		}

		public int getRequiredAcks(int defaultRequiredAcks) {
			return getProperty(REQUIRED_ACKS, defaultRequiredAcks);
		}

		public boolean getDefaultAutoCommitEnabled(boolean defaultAutoCommitEnabled) {
			return getProperty(AUTO_COMMIT_ENABLED, defaultAutoCommitEnabled);
		}

		public int getMinPartitionCount(int defaultPartitionCount) {
			return getProperty(BinderPropertyKeys.MIN_PARTITION_COUNT, defaultPartitionCount);
		}

	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		public ReceivingHandler() {
			this.setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Object handleRequestMessage(Message<?> requestMessage) {
			if (Mode.embeddedHeaders.equals(mode)) {
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

		private final PartitioningMetadata partitioningMetadata;

		private final AtomicInteger roundRobinCount = new AtomicInteger();

		private final String topicName;

		private final int numberOfKafkaPartitions;

		private final ProducerConfiguration<byte[], byte[]> producerConfiguration;


		private SendingHandler(String topicName, KafkaPropertiesAccessor properties, int numberOfPartitions,
				ProducerConfiguration<byte[], byte[]> producerConfiguration) {
			this.topicName = topicName;
			this.numberOfKafkaPartitions = numberOfPartitions;
			this.partitioningMetadata = new PartitioningMetadata(properties, numberOfPartitions);
			this.setBeanFactory(KafkaMessageChannelBinder.this.getBeanFactory());
			this.producerConfiguration = producerConfiguration;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			int targetPartition;
			if (partitioningMetadata.isPartitionedModule()) {
				targetPartition = determinePartition(message, partitioningMetadata);
			}
			else {
				targetPartition = roundRobin() % numberOfKafkaPartitions;
			}

			if (Mode.embeddedHeaders.equals(mode)) {
				MessageValues transformed = serializePayloadIfNecessary(message);
				byte[] messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
						KafkaMessageChannelBinder.this.headersToMap);
				producerConfiguration.send(topicName, targetPartition, null, messageToSend);
			}
			else if (Mode.raw.equals(mode)) {
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
