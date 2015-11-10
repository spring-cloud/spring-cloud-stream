/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import static org.springframework.util.MimeTypeUtils.ALL;
import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM;
import static org.springframework.util.MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE;
import static org.springframework.util.MimeTypeUtils.TEXT_PLAIN;
import static org.springframework.util.MimeTypeUtils.TEXT_PLAIN_VALUE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.AlternativeJdkIdGenerator;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.IdGenerator;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * @author David Turanski
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
public abstract class MessageChannelBinderSupport
		implements Binder<MessageChannel>, ApplicationContextAware, InitializingBean {

	protected static final String P2P_NAMED_CHANNEL_TYPE_PREFIX = "queue:";

	protected static final String PUBSUB_NAMED_CHANNEL_TYPE_PREFIX = "topic:";

	protected static final String JOB_CHANNEL_TYPE_PREFIX = "job:";

	protected static final String PARTITION_HEADER = "partition";

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private volatile AbstractApplicationContext applicationContext;

	private volatile Codec codec;

	private final StringConvertingContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private final ThreadLocal<Boolean> revertingDirectBinding = new ThreadLocal<Boolean>();

	protected static final List<MimeType> MEDIATYPES_MEDIATYPE_ALL = Collections.singletonList(ALL);

	private static final int DEFAULT_BACKOFF_INITIAL_INTERVAL = 1000;

	private static final int DEFAULT_BACKOFF_MAX_INTERVAL = 10000;

	private static final double DEFAULT_BACKOFF_MULTIPLIER = 2.0;

	private static final int DEFAULT_CONCURRENCY = 1;

	private static final int DEFAULT_MAX_ATTEMPTS = 3;

	private static final int DEFAULT_BATCH_SIZE = 50;

	private static final int DEFAULT_BATCH_BUFFER_LIMIT = 10000;

	private static final int DEFAULT_BATCH_TIMEOUT = 0;

	/**
	 * The set of properties every binder implementation must support (or at least tolerate).
	 */

	protected static final Set<Object> CONSUMER_STANDARD_PROPERTIES = new SetBuilder()
			.add(BinderProperties.COUNT)
			.add(BinderProperties.SEQUENCE)
			.build();

	protected static final Set<Object> PRODUCER_STANDARD_PROPERTIES = new HashSet<Object>(Arrays.asList(
			BinderProperties.NEXT_MODULE_COUNT,
			BinderProperties.NEXT_MODULE_CONCURRENCY
	));


	protected static final Set<Object> CONSUMER_RETRY_PROPERTIES = new HashSet<Object>(Arrays.asList(new String[] {
			BinderProperties.BACK_OFF_INITIAL_INTERVAL,
			BinderProperties.BACK_OFF_MAX_INTERVAL,
			BinderProperties.BACK_OFF_MULTIPLIER,
			BinderProperties.MAX_ATTEMPTS
	}));

	protected static final Set<Object> PRODUCER_PARTITIONING_PROPERTIES = new HashSet<Object>(
			Arrays.asList(new String[] {
					BinderProperties.PARTITION_KEY_EXPRESSION,
					BinderProperties.PARTITION_KEY_EXTRACTOR_CLASS,
					BinderProperties.PARTITION_SELECTOR_CLASS,
					BinderProperties.PARTITION_SELECTOR_EXPRESSION,
					BinderProperties.MIN_PARTITION_COUNT
			}));

	protected static final Set<Object> PRODUCER_BATCHING_BASIC_PROPERTIES = new HashSet<Object>(
			Arrays.asList(new String[] {
					BinderProperties.BATCHING_ENABLED,
					BinderProperties.BATCH_SIZE,
					BinderProperties.BATCH_TIMEOUT,
			}));

	protected static final Set<Object> PRODUCER_BATCHING_ADVANCED_PROPERTIES = new HashSet<Object>(
			Arrays.asList(new String[] {
					BinderProperties.BATCH_BUFFER_LIMIT,
			}));

	private final List<Binding> bindings = Collections.synchronizedList(new ArrayList<Binding>());

	private final IdGenerator idGenerator = new AlternativeJdkIdGenerator();

	protected volatile EvaluationContext evaluationContext;

	private volatile PartitionSelectorStrategy partitionSelector = new DefaultPartitionSelector();

	/**
	 * Used in the canonical case, when the binding does not involve an alias name.
	 */
	protected final SharedChannelProvider<DirectChannel> directChannelProvider = new
			SharedChannelProvider<DirectChannel>(
					DirectChannel.class) {

				@Override
				protected DirectChannel createSharedChannel(String name) {
					return new DirectChannel();
				}
			};

	protected volatile long defaultBackOffInitialInterval = DEFAULT_BACKOFF_INITIAL_INTERVAL;

	protected volatile long defaultBackOffMaxInterval = DEFAULT_BACKOFF_MAX_INTERVAL;

	protected volatile double defaultBackOffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;

	protected volatile int defaultConcurrency = DEFAULT_CONCURRENCY;

	protected volatile int defaultMaxAttempts = DEFAULT_MAX_ATTEMPTS;

	// properties for binder implementations that support batching

	protected volatile boolean defaultBatchingEnabled = false;

	protected volatile int defaultBatchSize = DEFAULT_BATCH_SIZE;

	protected volatile int defaultBatchBufferLimit = DEFAULT_BATCH_BUFFER_LIMIT;

	protected volatile long defaultBatchTimeout = DEFAULT_BATCH_TIMEOUT;

	// compression

	protected volatile boolean defaultCompress = false;

	protected volatile boolean defaultDurableSubscription = false;

	// Payload type cache
	private volatile Map<String, Class<?>> payloadTypeCache = new ConcurrentHashMap<>();

	/**
	 * For binder implementations that support a prefix, apply the prefix to the name.
	 * @param prefix the prefix.
	 * @param name the name.
	 */
	public static String applyPrefix(String prefix, String name) {
		return prefix + name;
	}

	/**
	 * For binder implementations that include a pub/sub component in identifiers, construct the name.
	 * @param name the name.
	 */
	public static String applyPubSub(String name) {
		return "topic." + name;
	}

	/**
	 * Build the requests entity name.
	 * @param name the name.
	 * @return the request entity name.
	 */
	public static String applyRequests(String name) {
		return name + ".requests";
	}

	/**
	 * For binder implementations that support dead lettering, construct the name of the dead letter entity for the
	 * underlying pipe name.
	 * @param name the name.
	 */
	public static String constructDLQName(String name) {
		return name + ".dlq";
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isInstanceOf(AbstractApplicationContext.class, applicationContext);
		this.applicationContext = (AbstractApplicationContext) applicationContext;
	}

	protected AbstractApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	protected ConfigurableListableBeanFactory getBeanFactory() {
		return this.applicationContext.getBeanFactory();
	}

	public void setCodec(Codec codec) {
		this.codec = codec;
	}

	protected IdGenerator getIdGenerator() {
		return this.idGenerator;
	}

	public void setIntegrationEvaluationContext(EvaluationContext evaluationContext) {
		this.evaluationContext = evaluationContext;
	}

	/**
	 * Set the partition strategy to be used by this binder if no partitionExpression is provided for a module.
	 * @param partitionSelector The selector.
	 */
	public void setPartitionSelector(PartitionSelectorStrategy partitionSelector) {
		this.partitionSelector = partitionSelector;
	}

	/**
	 * Set the default retry back off initial interval for this binder; can be overridden with consumer
	 * 'backOffInitialInterval' property.
	 * @param defaultBackOffInitialInterval
	 */
	public void setDefaultBackOffInitialInterval(long defaultBackOffInitialInterval) {
		this.defaultBackOffInitialInterval = defaultBackOffInitialInterval;
	}

	/**
	 * Set the default retry back off multiplier for this binder; can be overridden with consumer 'backOffMultiplier'
	 * property.
	 * @param defaultBackOffMultiplier
	 */
	public void setDefaultBackOffMultiplier(double defaultBackOffMultiplier) {
		this.defaultBackOffMultiplier = defaultBackOffMultiplier;
	}

	/**
	 * Set the default retry back off max interval for this binder; can be overridden with consumer 'backOffMaxInterval'
	 * property.
	 * @param defaultBackOffMaxInterval
	 */
	public void setDefaultBackOffMaxInterval(long defaultBackOffMaxInterval) {
		this.defaultBackOffMaxInterval = defaultBackOffMaxInterval;
	}

	/**
	 * Set the default concurrency for this binder; can be overridden with consumer 'concurrency' property.
	 * @param defaultConcurrency
	 */
	public void setDefaultConcurrency(int defaultConcurrency) {
		this.defaultConcurrency = defaultConcurrency;
	}

	/**
	 * The default maximum delivery attempts for this binder. Can be overridden by consumer property 'maxAttempts' if
	 * supported. Values less than 2 disable retry and one delivery attempt is made.
	 * @param defaultMaxAttempts The default maximum attempts.
	 */
	public void setDefaultMaxAttempts(int defaultMaxAttempts) {
		this.defaultMaxAttempts = defaultMaxAttempts;
	}

	/**
	 * Set whether this binder batches message sends by default. Only applies to binder implementations that support
	 * batching.
	 * @param defaultBatchingEnabled the defaultBatchingEnabled to set.
	 */
	public void setDefaultBatchingEnabled(boolean defaultBatchingEnabled) {
		this.defaultBatchingEnabled = defaultBatchingEnabled;
	}

	/**
	 * Set the default batch size; only applies when batching is enabled and the binder supports batching.
	 * @param defaultBatchSize the defaultBatchSize to set.
	 */
	public void setDefaultBatchSize(int defaultBatchSize) {
		this.defaultBatchSize = defaultBatchSize;
	}

	/**
	 * Set the default batch buffer limit - used to send a batch early if its size exceeds this. Only applies if
	 * batching is enabled and the binder supports this property.
	 * @param defaultBatchBufferLimit the defaultBatchBufferLimit to set.
	 */
	public void setDefaultBatchBufferLimit(int defaultBatchBufferLimit) {
		this.defaultBatchBufferLimit = defaultBatchBufferLimit;
	}

	/**
	 * Set the default batch timeout - used to send a batch if no messages arrive during this time. Only applies if
	 * batching is enabled and the binder supports this property.
	 * @param defaultBatchTimeout the defaultBatchTimeout to set.
	 */
	public void setDefaultBatchTimeout(long defaultBatchTimeout) {
		this.defaultBatchTimeout = defaultBatchTimeout;
	}

	/**
	 * Set whether compression will be used by producers, by default.
	 * @param defaultCompress 'true' to use compression.
	 */
	public void setDefaultCompress(boolean defaultCompress) {
		this.defaultCompress = defaultCompress;
	}

	/**
	 * Set whether subscriptions to taps/topics are durable.
	 * @param defaultDurableSubscription true for durable (default false).
	 */
	public void setDefaultDurableSubscription(boolean defaultDurableSubscription) {
		this.defaultDurableSubscription = defaultDurableSubscription;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.applicationContext, "The 'applicationContext' property cannot be null");
		if (this.evaluationContext == null) {
			this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
		}
	}

	/**
	 * Dynamically create a producer for the named channel.
	 * @param name The name.
	 * @param properties The properties.
	 * @return The channel.
	 */
	@Override
	public MessageChannel bindDynamicProducer(String name, Properties properties) {
		return doBindDynamicProducer(name, name, properties);
	}

	/**
	 * Create a producer for the named channel and bind it to the binder. Synchronized to avoid creating multiple
	 * instances.
	 * @param name The name.
	 * @param channelName The name of the channel to be created, and registered as bean.
	 * @param properties The properties.
	 * @return The channel.
	 */
	protected synchronized MessageChannel doBindDynamicProducer(String name, String channelName,
			Properties properties) {
		MessageChannel channel = this.directChannelProvider.lookupSharedChannel(channelName);
		if (channel == null) {
			try {
				channel = this.directChannelProvider.createAndRegisterChannel(channelName);
				bindProducer(name, channel, properties);
			}
			catch (RuntimeException e) {
				destroyCreatedChannel(channelName, channel);
				throw new BinderException(
						"Failed to bind dynamic channel '" + name + "' with properties " + properties, e);
			}
		}
		return channel;
	}

	/**
	 * Dynamically create a producer for the named channel. Note: even though it's pub/sub, we still use a direct
	 * channel. It will be bridged to a pub/sub channel in the local binder and bound to an appropriate element for other
	 * binders.
	 * @param name The name.
	 * @param properties The properties.
	 * @return The channel.
	 */
	@Override
	public MessageChannel bindDynamicPubSubProducer(String name, Properties properties) {
		return doBindDynamicPubSubProducer(name, name, properties);
	}

	/**
	 * Create a producer for the named channel and bind it to the binder. Synchronized to avoid creating multiple
	 * instances.
	 * @param name The name.
	 * @param channelName The name of the channel to be created, and registered as bean.
	 * @param properties The properties.
	 * @return The channel.
	 */
	protected synchronized MessageChannel doBindDynamicPubSubProducer(String name, String channelName,
			Properties properties) {
		MessageChannel channel = this.directChannelProvider.lookupSharedChannel(channelName);
		if (channel == null) {
			try {
				channel = this.directChannelProvider.createAndRegisterChannel(channelName);
				bindPubSubProducer(name, channel, properties);
			}
			catch (RuntimeException e) {
				destroyCreatedChannel(channelName, channel);
				throw new BinderException(
						"Failed to bind dynamic channel '" + name + "' with properties " + properties, e);
			}
		}
		return channel;
	}

	private void destroyCreatedChannel(String name, MessageChannel channel) {
		BeanFactory beanFactory = this.applicationContext.getBeanFactory();
		if (beanFactory.containsBean(name)) {
			if (beanFactory instanceof DefaultListableBeanFactory) {
				((DefaultListableBeanFactory) beanFactory).destroySingleton(name);
			}
		}
	}

	@Override
	public void unbindConsumers(String name) {
		deleteBindings("inbound." + name);
	}

	@Override
	public void unbindPubSubConsumers(String name, String group) {
		unbindConsumers(BinderUtils.groupedName(name, group));
	}

	@Override
	public void unbindProducers(String name) {
		deleteBindings("outbound." + name);
	}

	@Override
	public void unbindConsumer(String name, MessageChannel channel) {
		deleteBinding("inbound." + name, channel);
	}

	@Override
	public void unbindProducer(String name, MessageChannel channel) {
		deleteBinding("outbound." + name, channel);
	}

	protected void addBinding(Binding binding) {
		this.bindings.add(binding);
	}

	protected void deleteBindings(String name) {
		Assert.hasText(name, "a valid name is required to remove bindings");
		List<Binding> bindingsToRemove = new ArrayList<Binding>();
		synchronized (this.bindings) {
			Iterator<Binding> iterator = this.bindings.iterator();
			while (iterator.hasNext()) {
				Binding binding = iterator.next();
				if (binding.getEndpoint().getComponentName().equals(name)) {
					bindingsToRemove.add(binding);
				}
			}
			for (Binding binding : bindingsToRemove) {
				doDeleteBinding(binding);
			}
		}
	}

	protected void deleteBinding(String name, MessageChannel channel) {
		Assert.hasText(name, "a valid name is required to remove a binding");
		Assert.notNull(channel, "a valid channel is required to remove a binding");
		Binding bindingToRemove = null;
		synchronized (this.bindings) {
			Iterator<Binding> iterator = this.bindings.iterator();
			while (iterator.hasNext()) {
				Binding binding = iterator.next();
				if (binding.getChannel().equals(channel) &&
						binding.getEndpoint().getComponentName().equals(name)) {
					bindingToRemove = binding;
					break;
				}
			}
			if (bindingToRemove != null) {
				doDeleteBinding(bindingToRemove);
			}
		}

	}

	private void doDeleteBinding(Binding binding) {
		if (Binding.CONSUMER.equals(binding.getType())) {
			/*
			 * Revert the direct binding before stopping the consumer; the module
			 * outputChannel will temporarily have 2 subscribers.
			 */
			revertDirectBindingIfNecessary(binding);
		}
		binding.stop();
		this.bindings.remove(binding);
	}

	protected void stopBindings() {
		for (Lifecycle bean : this.bindings) {
			try {
				bean.stop();
			}
			catch (Exception e) {
				if (this.logger.isWarnEnabled()) {
					this.logger.warn("failed to stop adapter", e);
				}
			}
		}
	}

	protected final MessageValues serializePayloadIfNecessary(Message<?> message) {
		Object originalPayload = message.getPayload();
		Object originalContentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);

		//Pass content type as String since some transport adapters will exclude CONTENT_TYPE Header otherwise
		Object contentType = JavaClassMimeTypeConversion.mimeTypeFromObject(originalPayload).toString();
		Object payload = serializePayloadIfNecessary(originalPayload);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(payload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		if (originalContentType != null) {
			messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType);
		}
		return messageValues;
	}

	private byte[] serializePayloadIfNecessary(Object originalPayload) {
		if (originalPayload instanceof byte[]) {
			return (byte[]) originalPayload;
		}
		else {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				if (originalPayload instanceof String) {
					return ((String) originalPayload).getBytes("UTF-8");
				}
				this.codec.encode(originalPayload, bos);
				return bos.toByteArray();
			}
			catch (IOException e) {
				throw new SerializationFailedException("unable to serialize payload ["
						+ originalPayload.getClass().getName() + "]", e);
			}
		}
	}

	protected final MessageValues deserializePayloadIfNecessary(Message<?> message) {
		return deserializePayloadIfNecessary(new MessageValues(message));
	}

	protected final MessageValues deserializePayloadIfNecessary(MessageValues message) {
		Object originalPayload = message.getPayload();
		MimeType contentType = this.contentTypeResolver.resolve(message);
		Object payload = deserializePayload(originalPayload, contentType);
		if (payload != null) {
			message.setPayload(payload);

			Object originalContentType = message.get(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE);
			message.put(MessageHeaders.CONTENT_TYPE, originalContentType);
			message.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, null);
		}
		return message;
	}

	private Object deserializePayload(Object payload, MimeType contentType) {
		if (payload instanceof byte[]) {
			if (contentType == null || APPLICATION_OCTET_STREAM.equals(contentType)) {
				return payload;
			}
			else {
				return deserializePayload((byte[]) payload, contentType);
			}
		}
		return payload;
	}

	private Object deserializePayload(byte[] bytes, MimeType contentType) {
		if (TEXT_PLAIN.equals(contentType)) {
			try {
				return new String(bytes, "UTF-8");
			}
			catch (UnsupportedEncodingException e) {
				throw new SerializationFailedException("unable to deserialize [java.lang.String]. Encoding not supported.",
						e);
			}
		}
		else {
			String className = JavaClassMimeTypeConversion.classNameFromMimeType(contentType);
			try {
				// Cache types to avoid unnecessary ClassUtils.forName calls.
				Class<?> targetType = this.payloadTypeCache.get(className);
				if (targetType == null) {
					targetType = ClassUtils.forName(className, null);
					this.payloadTypeCache.put(className, targetType);
				}
				return this.codec.decode(bytes, targetType);
			}
			catch (ClassNotFoundException e) {
				throw new SerializationFailedException("unable to deserialize [" + className + "]. Class not found.",
						e);//NOSONAR
			}
			catch (IOException e) {
				throw new SerializationFailedException("unable to deserialize [" + className + "]", e);
			}
		}
	}

	/**
	 * Determine the partition to which to send this message. If a partition key extractor class is provided, it is
	 * invoked to determine the key. Otherwise, the partition key expression is evaluated to obtain the key value. If a
	 * partition selector class is provided, it will be invoked to determine the partition. Otherwise, if the partition
	 * expression is not null, it is evaluated against the key and is expected to return an integer to which the modulo
	 * function will be applied, using the partitionCount as the divisor. If no partition expression is provided, the
	 * key will be passed to the binder partition strategy along with the partitionCount. The default partition strategy
	 * uses {@code key.hashCode()}, and the result will be the mod of that value.
	 * @param message the message.
	 * @param meta the partitioning metadata.
	 * @return the partition.
	 */
	protected int determinePartition(Message<?> message, PartitioningMetadata meta) {
		Object key = null;
		if (StringUtils.hasText(meta.partitionKeyExtractorClass)) {
			key = invokeExtractor(meta.partitionKeyExtractorClass, message);
		}
		else if (meta.partitionKeyExpression != null) {
			key = meta.partitionKeyExpression.getValue(this.evaluationContext, message);
		}
		Assert.notNull(key, "Partition key cannot be null");
		int partition;
		if (StringUtils.hasText(meta.partitionSelectorClass)) {
			partition = invokePartitionSelector(meta.partitionSelectorClass, key, meta.partitionCount);
		}
		else if (meta.partitionSelectorExpression != null) {
			partition = meta.partitionSelectorExpression.getValue(this.evaluationContext, key, Integer.class);
		}
		else {
			partition = this.partitionSelector.selectPartition(key, meta.partitionCount);
		}
		partition = partition % meta.partitionCount;
		if (partition < 0) { // protection in case a user selector returns a negative.
			partition = Math.abs(partition);
		}
		return partition;
	}

	private Object invokeExtractor(String partitionKeyExtractorClassName, Message<?> message) {
		if (this.applicationContext.containsBean(partitionKeyExtractorClassName)) {
			return this.applicationContext.getBean(partitionKeyExtractorClassName, PartitionKeyExtractorStrategy.class)
					.extractKey(message);
		}
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(partitionKeyExtractorClassName, this.applicationContext.getClassLoader());
		}
		catch (Exception e) {
			this.logger.error("Failed to load key extractor", e);
			throw new BinderException("Failed to load key extractor: " + partitionKeyExtractorClassName, e);
		}
		try {
			Object extractor = clazz.newInstance();
			Assert.isInstanceOf(PartitionKeyExtractorStrategy.class, extractor);
			this.applicationContext.getBeanFactory().registerSingleton(partitionKeyExtractorClassName, extractor);
			this.applicationContext.getBeanFactory().initializeBean(extractor, partitionKeyExtractorClassName);
			return ((PartitionKeyExtractorStrategy) extractor).extractKey(message);
		}
		catch (Exception e) {
			this.logger.error("Failed to instantiate key extractor", e);
			throw new BinderException("Failed to instantiate key extractor: " + partitionKeyExtractorClassName, e);
		}
	}

	private int invokePartitionSelector(String partitionSelectorClassName, Object key, int partitionCount) {
		if (this.applicationContext.containsBean(partitionSelectorClassName)) {
			return this.applicationContext.getBean(partitionSelectorClassName, PartitionSelectorStrategy.class)
					.selectPartition(key, partitionCount);
		}
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(partitionSelectorClassName, this.applicationContext.getClassLoader());
		}
		catch (Exception e) {
			this.logger.error("Failed to load partition selector", e);
			throw new BinderException("Failed to load partition selector: " + partitionSelectorClassName, e);
		}
		try {
			Object extractor = clazz.newInstance();
			Assert.isInstanceOf(PartitionKeyExtractorStrategy.class, extractor);
			this.applicationContext.getBeanFactory().registerSingleton(partitionSelectorClassName, extractor);
			this.applicationContext.getBeanFactory().initializeBean(extractor, partitionSelectorClassName);
			return ((PartitionSelectorStrategy) extractor).selectPartition(key, partitionCount);
		}
		catch (Exception e) {
			this.logger.error("Failed to instantiate partition selector", e);
			throw new BinderException("Failed to instantiate partition selector: " + partitionSelectorClassName,
					e);
		}
	}

	/**
	 * Validate the provided deployment properties for the consumer against those supported by this binder implementation.
	 * The consumer is that part of the binder that consumes messages from the underlying infrastructure and sends them to
	 * the next module. Consumer properties are used to configure the consumer.
	 * @param name The name.
	 * @param properties The properties.
	 * @param supported The supported properties.
	 */
	protected void validateConsumerProperties(String name, Properties properties, Set<Object> supported) {
		if (properties != null) {
			validateProperties(name, properties, supported, "consumer");
		}
	}

	/**
	 * Validate the provided deployment properties for the producer against those supported by this binder implementation.
	 * When a module sends a message to the binder, the producer uses these properties while sending it to the underlying
	 * infrastructure.
	 * @param name The name.
	 * @param properties The properties.
	 * @param supported The supported properties.
	 */
	protected void validateProducerProperties(String name, Properties properties, Set<Object> supported) {
		if (properties != null) {
			validateProperties(name, properties, supported, "producer");
		}
	}

	private void validateProperties(String name, Properties properties, Set<Object> supported, String type) {
		StringBuilder builder = new StringBuilder();
		int errors = 0;
		for (Entry<Object, Object> entry : properties.entrySet()) {
			if (!supported.contains(entry.getKey())) {
				builder.append(entry.getKey()).append(",");
				errors++;
			}
		}
		if (errors > 0) {
			throw new IllegalArgumentException(getClass().getSimpleName() + " does not support "
					+ type
					+ " propert"
					+ (errors == 1 ? "y: " : "ies: ")
					+ builder.substring(0, builder.length() - 1)
					+ " for " + name + ".");
		}
	}

	protected String buildPartitionRoutingExpression(String expressionRoot) {
		return "'" + expressionRoot + "-' + headers['" + PARTITION_HEADER + "']";
	}

	/**
	 * Create and configure a retry template if the consumer 'maxAttempts' property is set.
	 * @param properties The properties.
	 * @return The retry template, or null if retry is not enabled.
	 */
	protected RetryTemplate buildRetryTemplateIfRetryEnabled(AbstractBinderPropertiesAccessor properties) {
		int maxAttempts = properties.getMaxAttempts(this.defaultMaxAttempts);
		if (maxAttempts > 1) {
			RetryTemplate template = new RetryTemplate();
			SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
			retryPolicy.setMaxAttempts(maxAttempts);
			ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
			backOffPolicy.setInitialInterval(properties.getBackOffInitialInterval(this.defaultBackOffInitialInterval));
			backOffPolicy.setMultiplier(properties.getBackOffMultiplier(this.defaultBackOffMultiplier));
			backOffPolicy.setMaxInterval(properties.getBackOffMaxInterval(this.defaultBackOffMaxInterval));
			template.setRetryPolicy(retryPolicy);
			template.setBackOffPolicy(backOffPolicy);
			return template;
		}
		else {
			return null;
		}
	}

	protected boolean isNamedChannel(String name) {
		return name.startsWith(PUBSUB_NAMED_CHANNEL_TYPE_PREFIX) || name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)
				|| name.startsWith(JOB_CHANNEL_TYPE_PREFIX);
	}

	/**
	 * Attempt to create a direct binding (avoiding the broker) if the consumer is local. Named channel producers are not
	 * bound directly.
	 * @param name The name.
	 * @param moduleOutputChannel The channel to bind.
	 * @param properties The producer properties.
	 * @return true if the producer is bound.
	 */
	protected boolean bindNewProducerDirectlyIfPossible(String name, SubscribableChannel moduleOutputChannel,
			AbstractBinderPropertiesAccessor properties) {
		if (!properties.isDirectBindingAllowed()) {
			return false;
		}
		else if (isNamedChannel(name)) {
			return false;
		}
		else if (this.revertingDirectBinding.get() != null) {
			// we're in the process of unbinding a direct binding
			this.revertingDirectBinding.remove();
			return false;
		}
		else {
			Binding consumerBinding = null;
			synchronized (this.bindings) {
				for (Binding binding : this.bindings) {
					if (binding.getName().equals(name) && Binding.CONSUMER.equals(binding.getType())) {
						consumerBinding = binding;
						break;
					}
				}
			}
			if (consumerBinding == null) {
				return false;
			}
			else {
				bindProducerDirectly(name, moduleOutputChannel, consumerBinding.getChannel(), properties);
				return true;
			}
		}
	}

	private void bindProducerDirectly(String name, SubscribableChannel producerChannel,
			MessageChannel consumerChannel, AbstractBinderPropertiesAccessor properties) {
		DirectHandler handler = new DirectHandler(consumerChannel);
		EventDrivenConsumer consumer = new EventDrivenConsumer(producerChannel, handler);
		consumer.setBeanFactory(getBeanFactory());
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		Binding binding = Binding.forDirectProducer(name, producerChannel, consumer, properties);
		addBinding(binding);
		binding.start();
		if (this.logger.isInfoEnabled()) {
			this.logger.info("Producer bound directly: " + binding);
		}
	}

	/**
	 * Attempt to bind a producer directly (avoiding the broker) if there is already a local producer. PubSub producers
	 * cannot be bound directly. Create the direct binding, then unbind the existing producer.
	 * @param name The name.
	 * @param consumerChannel The channel to bind the producer to.
	 */
	protected void bindExistingProducerDirectlyIfPossible(String name, MessageChannel consumerChannel) {
		if (!isNamedChannel(name)) {
			Binding producerBinding = null;
			synchronized (this.bindings) {
				for (Binding binding : this.bindings) {
					if (binding.getName().equals(name) && Binding.PRODUCER.equals(binding.getType())) {
						producerBinding = binding;
						break;
					}
				}
				if (producerBinding != null && producerBinding.getChannel() instanceof SubscribableChannel) {
					AbstractBinderPropertiesAccessor properties = producerBinding.getPropertiesAccessor();
					if (properties.isDirectBindingAllowed()) {
						bindProducerDirectly(name, (SubscribableChannel) producerBinding.getChannel(), consumerChannel,
								properties);
						producerBinding.stop();
						this.bindings.remove(producerBinding);
					}
				}
			}
		}
	}

	private void revertDirectBindingIfNecessary(Binding binding) {
		try {
			synchronized (this.bindings) { // Not necessary, called while synchronized, but just in case...
				Binding directBinding = null;
				Iterator<Binding> iterator = this.bindings.iterator();
				while (iterator.hasNext()) {
					Binding producer = iterator.next();
					if (Binding.DIRECT.equals(producer.getType()) && binding.getName().equals(producer.getName())) {
						this.revertingDirectBinding.set(Boolean.TRUE);
						bindProducer(producer.getName(), producer.getChannel(),
								producer.getPropertiesAccessor().getProperties());
						directBinding = producer;
						break;
					}
				}
				if (directBinding != null) {
					directBinding.stop();
					this.bindings.remove(directBinding);
					if (this.logger.isInfoEnabled()) {
						this.logger.info("direct binding reverted: " + directBinding);
					}
				}
			}
		}
		catch (Exception e) {
			this.logger.error("Could not revert direct binding: " + binding, e);
		}
	}

	/**
	 * Default partition strategy; only works on keys with "real" hash codes, such as String. Caller now always applies
	 * modulo so no need to do so here.
	 */
	private class DefaultPartitionSelector implements PartitionSelectorStrategy {

		@Override
		public int selectPartition(Object key, int partitionCount) {
			int hashCode = key.hashCode();
			if (hashCode == Integer.MIN_VALUE) {
				hashCode = 0;
			}
			return Math.abs(hashCode);
		}

	}

	protected static class PartitioningMetadata {

		private final String partitionKeyExtractorClass;

		private final Expression partitionKeyExpression;

		private final String partitionSelectorClass;

		private final Expression partitionSelectorExpression;

		private final int partitionCount;

		public PartitioningMetadata(AbstractBinderPropertiesAccessor properties, int partitionCount) {
			this.partitionCount = partitionCount;
			this.partitionKeyExtractorClass = properties.getPartitionKeyExtractorClass();
			this.partitionKeyExpression = properties.getPartitionKeyExpression();
			this.partitionSelectorClass = properties.getPartitionSelectorClass();
			this.partitionSelectorExpression = properties.getPartitionSelectorExpression();
		}

		public boolean isPartitionedModule() {
			return StringUtils.hasText(this.partitionKeyExtractorClass) || this.partitionKeyExpression != null;
		}

		public int getPartitionCount() {
			return this.partitionCount;
		}

	}

	/**
	 * Looks up or optionally creates a new channel to use.
	 * @author Eric Bottard
	 */
	protected abstract class SharedChannelProvider<T extends MessageChannel> {

		private final Class<T> requiredType;

		protected SharedChannelProvider(Class<T> clazz) {
			this.requiredType = clazz;
		}

		public synchronized final T lookupOrCreateSharedChannel(String name) {
			T channel = lookupSharedChannel(name);
			if (channel == null) {
				channel = createAndRegisterChannel(name);
			}
			return channel;
		}

		@SuppressWarnings("unchecked")
		public T createAndRegisterChannel(String name) {
			T channel = createSharedChannel(name);
			ConfigurableListableBeanFactory beanFactory = MessageChannelBinderSupport.this.applicationContext.getBeanFactory();
			beanFactory.registerSingleton(name, channel);
			channel = (T) beanFactory.initializeBean(channel, name);
			if (MessageChannelBinderSupport.this.logger.isDebugEnabled()) {
				MessageChannelBinderSupport.this.logger.debug("Registered channel:" + name);
			}
			return channel;
		}

		protected abstract T createSharedChannel(String name);

		public T lookupSharedChannel(String name) {
			T channel = null;
			if (MessageChannelBinderSupport.this.applicationContext.containsBean(name)) {
				try {
					channel = MessageChannelBinderSupport.this.applicationContext.getBean(name, this.requiredType);
				}
				catch (Exception e) {
					throw new IllegalArgumentException("bean '" + name
							+ "' is already registered but does not match the required type");
				}
			}
			return channel;
		}

	}

	/**
	 * Handles representing any java class as a {@link MimeType}.
	 * @author David Turanski
	 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/lang/Class.html#getName"/>
	 */
	abstract static class JavaClassMimeTypeConversion {

		public static final MimeType APPLICATION_OCTET_STREAM_MIME_TYPE = MimeType.valueOf
				(APPLICATION_OCTET_STREAM_VALUE);

		public static final MimeType TEXT_PLAIN_MIME_TYPE = MimeType.valueOf(TEXT_PLAIN_VALUE);

		private static ConcurrentMap<String, MimeType> mimeTypesCache = new ConcurrentHashMap<>();

		static MimeType mimeTypeFromObject(Object obj) {
			Assert.notNull(obj, "object cannot be null.");
			if (obj instanceof byte[]) {
				return APPLICATION_OCTET_STREAM_MIME_TYPE;
			}
			if (obj instanceof String) {
				return TEXT_PLAIN_MIME_TYPE;
			}
			String className = obj.getClass().getName();
			MimeType mimeType = mimeTypesCache.get(className);
			if (mimeType == null) {
				String modifiedClassName = className;
				if (obj.getClass().isArray()) {
					// Need to remove trailing ';' for an object array, e.g. "[Ljava.lang.String;" or multi-dimensional
					// "[[[Ljava.lang.String;"
					if (modifiedClassName.endsWith(";")) {
						modifiedClassName = modifiedClassName.substring(0, modifiedClassName.length() - 1);
					}
					// Wrap in quotes to handle the illegal '[' character
					modifiedClassName = "\"" + modifiedClassName + "\"";
				}
				mimeType = MimeType.valueOf("application/x-java-object;type=" + modifiedClassName);
				mimeTypesCache.put(className, mimeType);
			}
			return mimeType;
		}

		static String classNameFromMimeType(MimeType mimeType) {
			Assert.notNull(mimeType, "mimeType cannot be null.");
			String className = mimeType.getParameter("type");
			if (className == null) {
				return null;
			}
			//unwrap quotes if any
			className = className.replace("\"", "");

			// restore trailing ';'
			if (className.contains("[L")) {
				className += ";";
			}
			return className;
		}

	}

	public static class SetBuilder {

		private final Set<Object> set = new HashSet<Object>();

		public SetBuilder add(Object o) {
			this.set.add(o);
			return this;
		}

		public SetBuilder addAll(Set<Object> set) {
			this.set.addAll(set);
			return this;
		}

		public Set<Object> build() {
			return this.set;
		}

	}

	public static class DirectHandler implements MessageHandler {

		private final MessageChannel outputChannel;

		public DirectHandler(MessageChannel outputChannel) {
			this.outputChannel = outputChannel;
		}

		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			this.outputChannel.send(message);
		}

	}

	/**
	 * Perform manual acknowledgement based on the metadata stored in the binder.
	 */
	public void doManualAck(LinkedList<MessageHeaders> messageHeaders) {
	}

}
