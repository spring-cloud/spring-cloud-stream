/*
 * Copyright 2014-2015 the original author or authors.
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

package org.springframework.xd.dirt.integration.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.redis.inbound.RedisInboundChannelAdapter;
import org.springframework.integration.redis.inbound.RedisQueueMessageDrivenEndpoint;
import org.springframework.integration.redis.outbound.RedisPublishingMessageHandler;
import org.springframework.integration.redis.outbound.RedisQueueOutboundChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.xd.dirt.integration.bus.AbstractBusPropertiesAccessor;
import org.springframework.xd.dirt.integration.bus.Binding;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.EmbeddedHeadersMessageConverter;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.MessageBusSupport;
import org.springframework.xd.dirt.integration.bus.MessageValues;
import org.springframework.xd.dirt.integration.bus.XdHeaders;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;

/**
 * A {@link MessageBus} implementation backed by Redis.
 * @author Mark Fisher
 * @author Gary Russell
 * @author David Turanski
 * @author Jennifer Hickey
 */
public class RedisMessageBus extends MessageBusSupport implements DisposableBean {

	private static final String ERROR_HEADER = "errorKey";

	private static final SpelExpressionParser parser = new SpelExpressionParser();

	private final String[] headersToMap;

	/**
	 * Retry only.
	 */
	private static final Set<Object> SUPPORTED_PUBSUB_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.addAll(CONSUMER_RETRY_PROPERTIES)
			.build();

	/**
	 * Retry + concurrency.
	 */
	private static final Set<Object> SUPPORTED_NAMED_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.addAll(CONSUMER_RETRY_PROPERTIES)
			.add(BusProperties.CONCURRENCY)
			.build();

	/**
	 * Named + partitioning.
	 */
	private static final Set<Object> SUPPORTED_CONSUMER_PROPERTIES = new SetBuilder()
			.addAll(SUPPORTED_NAMED_CONSUMER_PROPERTIES)
			.add(BusProperties.PARTITION_INDEX)
			.build();

	/**
	 * Retry + concurrency (request).
	 */
	private static final Set<Object> SUPPORTED_REPLYING_CONSUMER_PROPERTIES = new SetBuilder()
			// request
			.addAll(CONSUMER_STANDARD_PROPERTIES)
			.addAll(CONSUMER_RETRY_PROPERTIES)
			.add(BusProperties.CONCURRENCY)
			.build();

	/**
	 * None.
	 */
	private static final Set<Object> SUPPORTED_PUBSUB_PRODUCER_PROPERTIES = PRODUCER_STANDARD_PROPERTIES;

	/**
	 * None.
	 */
	private static final Set<Object> SUPPORTED_NAMED_PRODUCER_PROPERTIES = PRODUCER_STANDARD_PROPERTIES;

	/**
	 * Partitioning.
	 */
	private static final Set<Object> SUPPORTED_PRODUCER_PROPERTIES = new SetBuilder()
			.addAll(PRODUCER_PARTITIONING_PROPERTIES)
			.addAll(PRODUCER_STANDARD_PROPERTIES)
			.add(BusProperties.DIRECT_BINDING_ALLOWED)
			.build();

	/**
	 * Retry, concurrency (reply).
	 */
	private static final Set<Object> SUPPORTED_REQUESTING_PRODUCER_PROPERTIES = new SetBuilder()
			// reply
			.addAll(CONSUMER_RETRY_PROPERTIES)
			.add(BusProperties.CONCURRENCY)
			.build();

	private final RedisConnectionFactory connectionFactory;

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new
			EmbeddedHeadersMessageConverter();

	private final RedisQueueOutboundChannelAdapter errorAdapter;

	public RedisMessageBus(RedisConnectionFactory connectionFactory, MultiTypeCodec<Object> codec) {
		this(connectionFactory, codec, new String[0]);
	}

	public RedisMessageBus(RedisConnectionFactory connectionFactory, MultiTypeCodec<Object> codec,
			String... headersToMap) {
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		Assert.notNull(codec, "codec must not be null");
		this.connectionFactory = connectionFactory;
		setCodec(codec);
		this.errorAdapter = new RedisQueueOutboundChannelAdapter(
				parser.parseExpression("headers['" + ERROR_HEADER + "']"), connectionFactory);
		if (headersToMap != null && headersToMap.length > 0) {
			String[] combinedHeadersToMap =
					Arrays.copyOfRange(XdHeaders.STANDARD_HEADERS, 0, XdHeaders.STANDARD_HEADERS.length
							+ headersToMap.length);
			System.arraycopy(headersToMap, 0, combinedHeadersToMap, XdHeaders.STANDARD_HEADERS.length,
					headersToMap.length);
			this.headersToMap = combinedHeadersToMap;
		}
		else {
			this.headersToMap = XdHeaders.STANDARD_HEADERS;
		}
	}

	@Override
	protected void onInit() {
		this.errorAdapter.setIntegrationEvaluationContext(this.evaluationContext);
	}

	@Override
	public void bindConsumer(final String name, MessageChannel moduleInputChannel, Properties properties) {
		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)) {
			validateConsumerProperties(name, properties, SUPPORTED_NAMED_CONSUMER_PROPERTIES);
		}
		else {
			validateConsumerProperties(name, properties, SUPPORTED_CONSUMER_PROPERTIES);
		}
		RedisPropertiesAccessor accessor = new RedisPropertiesAccessor(properties);
		String queueName = "queue." + name;
		int partitionIndex = accessor.getPartitionIndex();
		if (partitionIndex >= 0) {
			queueName += "-" + partitionIndex;
		}
		MessageProducerSupport adapter = createInboundAdapter(accessor, queueName);
		doRegisterConsumer(name, name + (partitionIndex >= 0 ? "-" + partitionIndex : ""), moduleInputChannel, adapter,
				accessor);
		bindExistingProducerDirectlyIfPossible(name, moduleInputChannel);
	}

	private MessageProducerSupport createInboundAdapter(RedisPropertiesAccessor accessor, String queueName) {
		MessageProducerSupport adapter;
		int concurrency = accessor.getConcurrency(this.defaultConcurrency);
		concurrency = concurrency > 0 ? concurrency : 1;
		if (concurrency == 1) {
			RedisQueueMessageDrivenEndpoint single = new RedisQueueMessageDrivenEndpoint(queueName,
					this.connectionFactory);
			single.setBeanFactory(getBeanFactory());
			single.setSerializer(null);
			adapter = single;
		}
		else {
			adapter = new CompositeRedisQueueMessageDrivenEndpoint(queueName, concurrency);
		}
		return adapter;
	}

	@Override
	public void bindPubSubConsumer(final String name, MessageChannel moduleInputChannel,
			Properties properties) {
		if (logger.isInfoEnabled()) {
			logger.info("declaring pubsub for inbound: " + name);
		}
		validateConsumerProperties(name, properties, SUPPORTED_PUBSUB_CONSUMER_PROPERTIES);
		RedisInboundChannelAdapter adapter = new RedisInboundChannelAdapter(this.connectionFactory);
		adapter.setBeanFactory(this.getBeanFactory());
		adapter.setSerializer(null);
		adapter.setTopics(applyPubSub(name));
		doRegisterConsumer(name, name, moduleInputChannel, adapter, new RedisPropertiesAccessor(properties));
	}

	private void doRegisterConsumer(String bindingName, String channelName, MessageChannel moduleInputChannel,
			MessageProducerSupport adapter, RedisPropertiesAccessor properties) {
		DirectChannel bridgeToModuleChannel = new DirectChannel();
		bridgeToModuleChannel.setBeanFactory(this.getBeanFactory());
		bridgeToModuleChannel.setBeanName(channelName + ".bridge");
		MessageChannel bridgeInputChannel = addRetryIfNeeded(channelName, bridgeToModuleChannel, properties);
		adapter.setOutputChannel(bridgeInputChannel);
		adapter.setBeanName("inbound." + bindingName);
		adapter.afterPropertiesSet();
		Binding consumerBinding = Binding.forConsumer(bindingName, adapter, moduleInputChannel, properties);
		addBinding(consumerBinding);
		ReceivingHandler convertingBridge = new ReceivingHandler();
		convertingBridge.setOutputChannel(moduleInputChannel);
		convertingBridge.setBeanName(channelName + ".bridge.handler");
		convertingBridge.afterPropertiesSet();
		bridgeToModuleChannel.subscribe(convertingBridge);
		consumerBinding.start();
	}

	/**
	 * If retry is enabled, wrap the bridge channel in another that will invoke send() within the scope of a retry
	 * template.
	 * @param name The name.
	 * @param bridgeToModuleChannel The channel.
	 * @param properties The properties.
	 * @return The channel, or a wrapper.
	 */
	private MessageChannel addRetryIfNeeded(final String name, final DirectChannel bridgeToModuleChannel,
			RedisPropertiesAccessor properties) {
		final RetryTemplate retryTemplate = buildRetryTemplateIfRetryEnabled(properties);
		if (retryTemplate == null) {
			return bridgeToModuleChannel;
		}
		else {
			DirectChannel channel = new DirectChannel() {

				@Override
				protected boolean doSend(final Message<?> message, final long timeout) {
					try {
						return retryTemplate.execute(new RetryCallback<Boolean, Exception>() {

							@Override
							public Boolean doWithRetry(RetryContext context) throws Exception {
								return bridgeToModuleChannel.send(message, timeout);
							}

						}, new RecoveryCallback<Boolean>() {

							/**
							 * Send the failed message to 'ERRORS:[name]'.
							 */
							@Override
							public Boolean recover(RetryContext context) throws Exception {
								logger.error(
										"Failed to deliver message; retries exhausted; message sent to queue 'ERRORS:"
												+ name + "' " + context.getLastThrowable());
								errorAdapter.handleMessage(getMessageBuilderFactory().fromMessage(message)
										.setHeader(ERROR_HEADER, "ERRORS:" + name)
										.build());
								return true;
							}

						});
					}
					catch (Exception e) {
						logger.error("Failed to deliver message", e);
						return false;
					}
				}

			};
			channel.setBeanName(name + ".bridge");
			return channel;
		}
	}

	@Override
	public void bindProducer(final String name, MessageChannel moduleOutputChannel,
			Properties properties) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		if (name.startsWith(P2P_NAMED_CHANNEL_TYPE_PREFIX)) {
			validateProducerProperties(name, properties, SUPPORTED_NAMED_PRODUCER_PROPERTIES);
		}
		else {
			validateProducerProperties(name, properties, SUPPORTED_PRODUCER_PROPERTIES);
		}
		RedisPropertiesAccessor accessor = new RedisPropertiesAccessor(properties);
		if (!bindNewProducerDirectlyIfPossible(name, (SubscribableChannel) moduleOutputChannel, accessor)) {
			String partitionKeyExtractorClass = accessor.getPartitionKeyExtractorClass();
			Expression partitionKeyExpression = accessor.getPartitionKeyExpression();
			RedisQueueOutboundChannelAdapter queue;
			String queueName = "queue." + name;
			if (partitionKeyExpression == null && !StringUtils.hasText(partitionKeyExtractorClass)) {
				queue = new RedisQueueOutboundChannelAdapter(queueName, this.connectionFactory);
			}
			else {
				queue = new RedisQueueOutboundChannelAdapter(
						parser.parseExpression(buildPartitionRoutingExpression(queueName)), this.connectionFactory);
			}
			queue.setIntegrationEvaluationContext(this.evaluationContext);
			queue.setBeanFactory(this.getBeanFactory());
			queue.afterPropertiesSet();
			doRegisterProducer(name, moduleOutputChannel, queue, accessor);
		}
	}

	@Override
	public void bindPubSubProducer(final String name, MessageChannel moduleOutputChannel,
			Properties properties) {
		validateProducerProperties(name, properties, SUPPORTED_PUBSUB_PRODUCER_PROPERTIES);
		RedisPublishingMessageHandler topic = new RedisPublishingMessageHandler(connectionFactory);
		topic.setBeanFactory(this.getBeanFactory());
		topic.setTopic(applyPubSub(name));
		topic.afterPropertiesSet();
		doRegisterProducer(name, moduleOutputChannel, topic, new RedisPropertiesAccessor(properties));
	}

	private void doRegisterProducer(final String name, MessageChannel moduleOutputChannel, MessageHandler delegate,
			RedisPropertiesAccessor properties) {
		this.doRegisterProducer(name, moduleOutputChannel, delegate, null, properties);
	}

	private void doRegisterProducer(final String name, MessageChannel moduleOutputChannel, MessageHandler delegate,
			String replyTo, RedisPropertiesAccessor properties) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		MessageHandler handler = new SendingHandler(delegate, replyTo, properties);
		EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler);
		consumer.setBeanFactory(this.getBeanFactory());
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		Binding producerBinding = Binding.forProducer(name, moduleOutputChannel, consumer, properties);
		addBinding(producerBinding);
		producerBinding.start();
	}

	@Override
	public void bindRequestor(String name, MessageChannel requests, MessageChannel replies,
			Properties properties) {
		if (logger.isInfoEnabled()) {
			logger.info("binding requestor: " + name);
		}
		Assert.isInstanceOf(SubscribableChannel.class, requests);
		validateProducerProperties(name, properties, SUPPORTED_REQUESTING_PRODUCER_PROPERTIES);
		RedisQueueOutboundChannelAdapter queue = new RedisQueueOutboundChannelAdapter("queue." + applyRequests(name),
				this.connectionFactory);
		queue.setBeanFactory(this.getBeanFactory());
		queue.afterPropertiesSet();
		String replyQueueName = name + ".replies." + this.getIdGenerator().generateId();
		RedisPropertiesAccessor accessor = new RedisPropertiesAccessor(properties);
		this.doRegisterProducer(name, requests, queue, replyQueueName, accessor);
		MessageProducerSupport adapter = createInboundAdapter(accessor, replyQueueName);
		this.doRegisterConsumer(name, name, replies, adapter, accessor);
	}

	@Override
	public void bindReplier(String name, MessageChannel requests, MessageChannel replies,
			Properties properties) {
		if (logger.isInfoEnabled()) {
			logger.info("binding replier: " + name);
		}
		validateConsumerProperties(name, properties, SUPPORTED_REPLYING_CONSUMER_PROPERTIES);
		RedisPropertiesAccessor accessor = new RedisPropertiesAccessor(properties);
		MessageProducerSupport adapter = createInboundAdapter(accessor, "queue." + applyRequests(name));
		this.doRegisterConsumer(name, name, requests, adapter, accessor);

		RedisQueueOutboundChannelAdapter replyQueue = new RedisQueueOutboundChannelAdapter(
				RedisMessageBus.parser.parseExpression("headers['" + XdHeaders.REPLY_TO + "']"),
				this.connectionFactory);
		replyQueue.setBeanFactory(this.getBeanFactory());
		replyQueue.setIntegrationEvaluationContext(this.evaluationContext);
		replyQueue.afterPropertiesSet();
		this.doRegisterProducer(name, replies, replyQueue, accessor);
	}

	@Override
	public void destroy() {
		stopBindings();
	}

	private class SendingHandler extends AbstractMessageHandler {

		private final MessageHandler delegate;

		private final String replyTo;

		private final PartitioningMetadata partitioningMetadata;


		private SendingHandler(MessageHandler delegate, String replyTo, RedisPropertiesAccessor properties) {
			this.delegate = delegate;
			this.replyTo = replyTo;
			this.partitioningMetadata = new PartitioningMetadata(properties, properties.getNextModuleCount());
			this.setBeanFactory(RedisMessageBus.this.getBeanFactory());
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			MessageValues transformed = serializePayloadIfNecessary(message);

			if (replyTo != null) {
				transformed.put(XdHeaders.REPLY_TO, this.replyTo);
			}
			if (this.partitioningMetadata.isPartitionedModule()) {

				transformed.put(PARTITION_HEADER, determinePartition(message, this.partitioningMetadata));
			}
			
			byte[] messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
					RedisMessageBus.this.headersToMap);
			delegate.handleMessage(MessageBuilder.withPayload(messageToSend).copyHeaders(transformed).build());
		}

	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		public ReceivingHandler() {
			super();
			this.setBeanFactory(RedisMessageBus.this.getBeanFactory());
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			MessageValues theRequestMessage;
			try {
				theRequestMessage = embeddedHeadersMessageConverter.extractHeaders((Message<byte[]>) requestMessage, true);
			}
			catch (Exception e) {
				logger.error(EmbeddedHeadersMessageConverter.decodeExceptionMessage(requestMessage), e);
				theRequestMessage = new MessageValues(requestMessage);
			}
			return deserializePayloadIfNecessary(theRequestMessage).toMessage(getMessageBuilderFactory());
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			// prevent returned message from being copied in superclass
			return false;
		}
	}

	private static class RedisPropertiesAccessor extends AbstractBusPropertiesAccessor {

		public RedisPropertiesAccessor(Properties properties) {
			super(properties);
		}

	}

	/**
	 * Provides concurrency by creating a list of message-driven endpoints.
	 */
	private class CompositeRedisQueueMessageDrivenEndpoint extends MessageProducerSupport {

		private final List<RedisQueueMessageDrivenEndpoint> consumers = new
				ArrayList<RedisQueueMessageDrivenEndpoint>();

		public CompositeRedisQueueMessageDrivenEndpoint(String queueName, int concurrency) {
			for (int i = 0; i < concurrency; i++) {
				RedisQueueMessageDrivenEndpoint adapter = new RedisQueueMessageDrivenEndpoint(queueName,
						connectionFactory);
				adapter.setBeanFactory(RedisMessageBus.this.getBeanFactory());
				adapter.setSerializer(null);
				adapter.setBeanName("inbound." + queueName + "." + i);
				this.consumers.add(adapter);
			}
			this.setBeanFactory(RedisMessageBus.this.getBeanFactory());
		}

		@Override
		protected void onInit() {
			for (RedisQueueMessageDrivenEndpoint consumer : consumers) {
				consumer.afterPropertiesSet();
			}
		}

		@Override
		protected void doStart() {
			for (RedisQueueMessageDrivenEndpoint consumer : consumers) {
				consumer.start();
			}
		}

		@Override
		protected void doStop() {
			for (RedisQueueMessageDrivenEndpoint consumer : consumers) {
				consumer.stop();
			}
		}

		@Override
		public void setOutputChannel(MessageChannel outputChannel) {
			for (RedisQueueMessageDrivenEndpoint consumer : consumers) {
				consumer.setOutputChannel(outputChannel);
			}
		}

		@Override
		public void setErrorChannel(MessageChannel errorChannel) {
			for (RedisQueueMessageDrivenEndpoint consumer : consumers) {
				consumer.setErrorChannel(errorChannel);
			}
		}

	}

}
