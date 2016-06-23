/*
 * Copyright 2013-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.aopalliance.aop.Advice;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.LocalizedQueueConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A {@link org.springframework.cloud.stream.binder.Binder} implementation backed by RabbitMQ.
 * @author Mark Fisher
 * @author Gary Russell
 * @author Jennifer Hickey
 * @author Gunnar Hillert
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Marius Bogoevici
 */
public class RabbitMessageChannelBinder
		extends AbstractMessageChannelBinder<ExtendedConsumerProperties<RabbitConsumerProperties>,
		ExtendedProducerProperties<RabbitProducerProperties>, Queue>
		implements ExtendedPropertiesBinder<MessageChannel, RabbitConsumerProperties, RabbitProducerProperties> {

	private static final AnonymousQueue.Base64UrlNamingStrategy ANONYMOUS_GROUP_NAME_GENERATOR
			= new AnonymousQueue.Base64UrlNamingStrategy("anonymous.");

	private static final String DEAD_LETTER_EXCHANGE = "DLX";

	private static final MessagePropertiesConverter inboundMessagePropertiesConverter =
			new DefaultMessagePropertiesConverter() {

				@Override
				public MessageProperties toMessageProperties(AMQP.BasicProperties source, Envelope envelope,
						String charset) {
					MessageProperties properties = super.toMessageProperties(source, envelope, charset);
					properties.setDeliveryMode(null);
					return properties;
				}
			};

	private final RabbitAdmin rabbitAdmin;

	private final GenericApplicationContext autoDeclareContext = new GenericApplicationContext();

	private final RabbitProperties rabbitProperties;

	private ConnectionFactory connectionFactory;

	private MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private MessagePostProcessor compressingPostProcessor = new GZipPostProcessor();

	private volatile String[] adminAddresses;

	private volatile String[] nodes;

	private volatile boolean clustered;

	private RabbitExtendedBindingProperties extendedBindingProperties = new RabbitExtendedBindingProperties();

	public RabbitMessageChannelBinder(ConnectionFactory connectionFactory, RabbitProperties rabbitProperties) {
		super(true, new String[0]);
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		Assert.notNull(rabbitProperties, "rabbitProperties must not be null");
		this.connectionFactory = connectionFactory;
		this.rabbitProperties = rabbitProperties;
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
		this.autoDeclareContext.refresh();
		this.rabbitAdmin.setApplicationContext(this.autoDeclareContext);
		this.rabbitAdmin.setIgnoreDeclarationExceptions(true);
		this.rabbitAdmin.afterPropertiesSet();
	}

	/**
	 * Set a {@link MessagePostProcessor} to decompress messages. Defaults to a
	 * {@link DelegatingDecompressingPostProcessor} with its default delegates.
	 * @param decompressingPostProcessor the post processor.
	 */
	public void setDecompressingPostProcessor(MessagePostProcessor decompressingPostProcessor) {
		this.decompressingPostProcessor = decompressingPostProcessor;
	}

	/**
	 * Set a {@link org.springframework.amqp.core.MessagePostProcessor} to compress messages. Defaults to a
	 * {@link org.springframework.amqp.support.postprocessor.GZipPostProcessor}.
	 * @param compressingPostProcessor the post processor.
	 */
	public void setCompressingPostProcessor(MessagePostProcessor compressingPostProcessor) {
		this.compressingPostProcessor = compressingPostProcessor;
	}

	public void setAdminAddresses(String[] adminAddresses) {
		this.adminAddresses = Arrays.copyOf(adminAddresses, adminAddresses.length);
	}

	public void setNodes(String[] nodes) {
		this.nodes = Arrays.copyOf(nodes, nodes.length);
		this.clustered = nodes.length > 1;
	}

	public void setExtendedBindingProperties(RabbitExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	@Override
	public void onInit() {
		if (this.clustered) {
			String[] addresses = StringUtils.commaDelimitedListToStringArray(this.rabbitProperties.getAddresses());
			Assert.state(addresses.length == this.adminAddresses.length
							&& addresses.length == this.nodes.length,
					"'addresses', 'adminAddresses', and 'nodes' properties must have equal length");
			this.connectionFactory = new LocalizedQueueConnectionFactory(this.connectionFactory, addresses,
					this.adminAddresses, this.nodes, this.rabbitProperties.getVirtualHost(),
					this.rabbitProperties.getUsername(), this.rabbitProperties.getPassword(),
					this.rabbitProperties.getSsl().isEnabled(), this.rabbitProperties.getSsl().getKeyStore(),
					this.rabbitProperties.getSsl().getTrustStore(),
					this.rabbitProperties.getSsl().getKeyStorePassword(),
					this.rabbitProperties.getSsl().getTrustStorePassword());
		}
	}

	@Override
	public RabbitConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}


	@Override
	public RabbitProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	protected MessageProducer createConsumerEndpoint(String name, String group, Queue destination,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {

		DirectChannel convertingBridgeChannel = new DirectChannel();
		convertingBridgeChannel.setBeanFactory(this.getBeanFactory());
		final String baseQueueName = baseQueueName(name, group);
		convertingBridgeChannel.setBeanName(baseQueueName + ".bridge");

		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(
				this.connectionFactory);
		listenerContainer.setAcknowledgeMode(properties.getExtension().getAcknowledgeMode());
		listenerContainer.setChannelTransacted(properties.getExtension().isTransacted());
		listenerContainer.setDefaultRequeueRejected(properties.getExtension().isRequeueRejected());
		int concurrency = properties.getConcurrency();
		concurrency = concurrency > 0 ? concurrency : 1;
		listenerContainer.setConcurrentConsumers(concurrency);
		int maxConcurrency = properties.getExtension().getMaxConcurrency();
		if (maxConcurrency > concurrency) {
			listenerContainer.setMaxConcurrentConsumers(maxConcurrency);
		}
		listenerContainer.setPrefetchCount(properties.getExtension().getPrefetch());
		listenerContainer.setRecoveryInterval(properties.getExtension().getRecoveryInterval());
		listenerContainer.setTxSize(properties.getExtension().getTxSize());
		listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(destination.getName() + "-"));
		listenerContainer.setQueues((Queue) destination);
		if (properties.getMaxAttempts() > 1 || properties.getExtension().isRepublishToDlq()) {
			RetryOperationsInterceptor retryInterceptor = RetryInterceptorBuilder.stateless()
					.retryOperations(buildRetryTemplate(properties))
					.recoverer(determineRecoverer(baseQueueName, properties.getExtension().getPrefix(),
							properties.getExtension().isRepublishToDlq()))
					.build();
			listenerContainer.setAdviceChain(new Advice[] {retryInterceptor});
		}
		listenerContainer.setAfterReceivePostProcessors(this.decompressingPostProcessor);
		listenerContainer.setMessagePropertiesConverter(RabbitMessageChannelBinder.inboundMessagePropertiesConverter);
		listenerContainer.afterPropertiesSet();

		AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(listenerContainer);
		adapter.setBeanFactory(this.getBeanFactory());
		adapter.setBeanName("inbound." + baseQueueName);
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.inboundMapper();
		mapper.setRequestHeaderNames(properties.getExtension().getRequestHeaderPatterns());
		mapper.setReplyHeaderNames(properties.getExtension().getReplyHeaderPatterns());
		adapter.setHeaderMapper(mapper);
		adapter.afterPropertiesSet();
		return adapter;
	}

	@Override
	protected void afterUnbindConsumer(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {
		cleanAutoDeclareContext(consumerProperties.getExtension().getPrefix(), baseQueueName(name, group));
	}

	@Override
	protected Queue createConsumerDestinationIfNecessary(String name, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		boolean anonymous = !StringUtils.hasText(group);
		String baseQueueName = anonymous ? groupedName(name, ANONYMOUS_GROUP_NAME_GENERATOR.generateName())
				: groupedName(name, group);
		if (this.logger.isInfoEnabled()) {
			this.logger.info("declaring queue for inbound: " + baseQueueName + ", bound to: " + name);
		}
		String prefix = properties.getExtension().getPrefix();
		String exchangeName = applyPrefix(prefix, name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);

		String queueName = applyPrefix(prefix, baseQueueName);
		boolean partitioned = !anonymous && properties.isPartitioned();
		boolean durable = !anonymous && properties.getExtension().isDurableSubscription();
		Queue queue;

		if (anonymous) {
			queue = new Queue(queueName, false, true, true);
		}
		else {
			if (partitioned) {
				String partitionSuffix = "-" + properties.getInstanceIndex();
				queueName += partitionSuffix;
			}
			if (durable) {
				queue = new Queue(queueName, true, false, false,
						queueArgs(queueName, properties.getExtension().getPrefix(),
								properties.getExtension().isAutoBindDlq()));
			}
			else {
				queue = new Queue(queueName, false, false, true);
			}
		}
		declareQueue(queueName, queue);
		if (partitioned) {
			String bindingKey = String.format("%s-%d", name, properties.getInstanceIndex());
			declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with(bindingKey));
		}
		else {
			declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange).with("#"));
		}
		if (durable) {
			autoBindDLQ(applyPrefix(properties.getExtension().getPrefix(), baseQueueName), queueName,
					properties.getExtension().getPrefix(), properties.getExtension().isAutoBindDlq());
		}
		return queue;
	}

	private String baseQueueName(String name, String group) {
		return !StringUtils.hasText(group) ? groupedName(name, ANONYMOUS_GROUP_NAME_GENERATOR.generateName())
				: groupedName(name, group);
	}

	private Map<String, Object> queueArgs(String queueName, String prefix, boolean bindDlq) {
		Map<String, Object> args = new HashMap<>();
		if (bindDlq) {
			args.put("x-dead-letter-exchange", applyPrefix(prefix, "DLX"));
			args.put("x-dead-letter-routing-key", queueName);
		}
		return args;
	}

	private MessageRecoverer determineRecoverer(String name, String prefix, boolean republish) {
		if (republish) {
			RabbitTemplate errorTemplate = new RabbitTemplate(this.connectionFactory);
			return new RepublishMessageRecoverer(errorTemplate,
					deadLetterExchangeName(prefix),
					applyPrefix(prefix, name));
		}
		else {
			return new RejectAndDontRequeueRecoverer();
		}
	}

	@Override
	protected void createProducerDestinationIfNecessary(String name,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties) {
		String exchangeName = applyPrefix(producerProperties.getExtension().getPrefix(), name);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);
	}

	@Override
	protected MessageHandler createProducerMessageHandler(final String destination,
			ExtendedProducerProperties<RabbitProducerProperties> properties)
			throws Exception {
		String prefix = properties.getExtension().getPrefix();
		String exchangeName = applyPrefix(prefix, destination);
		TopicExchange exchange = new TopicExchange(exchangeName);
		declareExchange(exchangeName, exchange);
		final AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(buildRabbitTemplate(properties.getExtension()));
		endpoint.setExchangeName(exchange.getName());
		if (!properties.isPartitioned()) {
			endpoint.setRoutingKey(destination);
		}
		else {
			endpoint.setRoutingKeyExpression(EXPRESSION_PARSER.parseExpression(buildPartitionRoutingExpression(
					destination)));
		}
		for (String requiredGroupName : properties.getRequiredGroups()) {
			String baseQueueName = exchangeName + "." + requiredGroupName;
			if (!properties.isPartitioned()) {
				Queue queue = new Queue(baseQueueName, true, false, false,
						queueArgs(baseQueueName, prefix, properties.getExtension().isAutoBindDlq()));
				declareQueue(baseQueueName, queue);
				autoBindDLQ(baseQueueName, baseQueueName, properties.getExtension().getPrefix(),
						properties.getExtension().isAutoBindDlq());
				org.springframework.amqp.core.Binding binding = BindingBuilder.bind(queue).to(exchange).with(
						destination);
				declareBinding(baseQueueName, binding);
			}
			else {
				// if the stream is partitioned, create one queue for each target partition for the default group
				for (int i = 0; i < properties.getPartitionCount(); i++) {
					String partitionSuffix = "-" + i;
					String partitionQueueName = baseQueueName + partitionSuffix;
					Queue queue = new Queue(partitionQueueName, true, false, false,
							queueArgs(partitionQueueName, properties.getExtension().getPrefix(),
									properties.getExtension().isAutoBindDlq()));
					declareQueue(queue.getName(), queue);
					autoBindDLQ(baseQueueName, baseQueueName + partitionSuffix, properties.getExtension().getPrefix(),
							properties.getExtension().isAutoBindDlq());
					declareBinding(queue.getName(), BindingBuilder.bind(queue).to(exchange)
							.with(destination + partitionSuffix));
				}
			}
		}
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.outboundMapper();
		mapper.setRequestHeaderNames(properties.getExtension().getRequestHeaderPatterns());
		mapper.setReplyHeaderNames(properties.getExtension().getReplyHeaderPatterns());
		endpoint.setHeaderMapper(mapper);
		endpoint.setDefaultDeliveryMode(properties.getExtension().getDeliveryMode());
		endpoint.setBeanFactory(this.getBeanFactory());
		endpoint.afterPropertiesSet();
		return endpoint;
	}


	private RabbitTemplate buildRabbitTemplate(RabbitProducerProperties properties) {
		RabbitTemplate rabbitTemplate;
		if (properties.isBatchingEnabled()) {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
					properties.getBatchSize(),
					properties.getBatchBufferLimit(),
					properties.getBatchTimeout());
			rabbitTemplate = new BatchingRabbitTemplate(batchingStrategy,
					getApplicationContext().getBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME,
							TaskScheduler.class));
		}
		else {
			rabbitTemplate = new RabbitTemplate();
		}
		rabbitTemplate.setConnectionFactory(this.connectionFactory);
		if (properties.isCompress()) {
			rabbitTemplate.setBeforePublishPostProcessors(this.compressingPostProcessor);
		}
		rabbitTemplate.setChannelTransacted(properties.isTransacted());
		rabbitTemplate.afterPropertiesSet();
		return rabbitTemplate;
	}

	/**
	 * If so requested, declare the DLX/DLQ and bind it. The DLQ is bound to the DLX with a routing key of the original
	 * queue name because we use default exchange routing by queue name for the original message.
	 * @param queueName   The base name for the queue (including the binder prefix, if any).
	 * @param routingKey  The routing key for the queue.
	 * @param autoBindDlq true if the DLQ should be bound.
	 */
	private void autoBindDLQ(final String queueName, String routingKey, String prefix, boolean autoBindDlq) {
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("autoBindDLQ=" + autoBindDlq
					+ " for: " + queueName);
		}
		if (autoBindDlq) {
			String dlqName = constructDLQName(queueName);
			Queue dlq = new Queue(dlqName);
			declareQueue(dlqName, dlq);
			final String dlxName = deadLetterExchangeName(prefix);
			final DirectExchange dlx = new DirectExchange(dlxName);
			declareExchange(dlxName, dlx);
			declareBinding(dlqName, BindingBuilder.bind(dlq).to(dlx).with(routingKey));
		}
	}

	private void declareQueue(String beanName, Queue queue) {
		try {
			this.rabbitAdmin.declareQueue(queue);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Declaration of queue: " + queue.getName() + " deferred - connection not available");
			}
		}
		addToAutoDeclareContext(beanName, queue);
	}

	private void declareExchange(final String rootName, final Exchange exchange) {
		try {
			this.rabbitAdmin.declareExchange(exchange);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of exchange: " + exchange.getName() + " deferred - connection not available");
			}
		}
		addToAutoDeclareContext(rootName + ".exchange", exchange);
	}

	private void declareBinding(String rootName, org.springframework.amqp.core.Binding binding) {
		try {
			this.rabbitAdmin.declareBinding(binding);
		}
		catch (AmqpConnectException e) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"Declaration of binding: " + rootName + ".binding deferred - connection not available");
			}
		}
		addToAutoDeclareContext(rootName + ".binding", binding);
	}

	private String deadLetterExchangeName(String prefix) {
		return prefix + DEAD_LETTER_EXCHANGE;
	}

	private void addToAutoDeclareContext(String name, Object bean) {
		synchronized (this.autoDeclareContext) {
			if (!this.autoDeclareContext.containsBean(name)) {
				this.autoDeclareContext.getBeanFactory().registerSingleton(name, bean);
			}
		}
	}

	private void cleanAutoDeclareContext(String prefix, String name) {
		synchronized (this.autoDeclareContext) {
			removeSingleton(applyPrefix(prefix, name) + ".binding");
			removeSingleton(applyPrefix(prefix, name));
			String dlq = applyPrefix(prefix, name) + ".dlq";
			removeSingleton(dlq + ".binding");
			removeSingleton(dlq);
		}
	}

	private void removeSingleton(String name) {
		if (this.autoDeclareContext.containsBean(name)) {
			ConfigurableListableBeanFactory beanFactory = this.autoDeclareContext.getBeanFactory();
			if (beanFactory instanceof DefaultListableBeanFactory) {
				((DefaultListableBeanFactory) beanFactory).destroySingleton(name);
			}
		}
	}
}
