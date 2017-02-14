/*
 * Copyright 2013-2017 the original author or authors.
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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.LocalizedQueueConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
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
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
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
		ExtendedProducerProperties<RabbitProducerProperties>, RabbitExchangeQueueProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, RabbitConsumerProperties, RabbitProducerProperties> {

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

	private final RabbitProperties rabbitProperties;

	private ConnectionFactory connectionFactory;

	private MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private MessagePostProcessor compressingPostProcessor = new GZipPostProcessor();

	private volatile String[] adminAddresses;

	private volatile String[] nodes;

	private volatile boolean clustered;

	private RabbitExtendedBindingProperties extendedBindingProperties = new RabbitExtendedBindingProperties();

	public RabbitMessageChannelBinder(ConnectionFactory connectionFactory, RabbitProperties rabbitProperties,
										RabbitExchangeQueueProvisioner provisioningProvider) {
		super(true, new String[0], provisioningProvider);
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		Assert.notNull(rabbitProperties, "rabbitProperties must not be null");
		this.connectionFactory = connectionFactory;
		this.rabbitProperties = rabbitProperties;
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
	protected MessageHandler createProducerMessageHandler(final ProducerDestination producerDestination,
														ExtendedProducerProperties<RabbitProducerProperties> producerProperties)
			throws Exception {
		String prefix = producerProperties.getExtension().getPrefix();
		String exchangeName = producerDestination.getName();
		String destination = StringUtils.isEmpty(prefix) ? exchangeName : exchangeName.substring(prefix.length());
		final AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(buildRabbitTemplate(producerProperties.getExtension()));
		endpoint.setExchangeName(producerDestination.getName());
		RabbitProducerProperties extendedProperties = producerProperties.getExtension();
		String routingKeyExpression = extendedProperties.getRoutingKeyExpression();
		if (!producerProperties.isPartitioned()) {
			if (routingKeyExpression == null) {
				endpoint.setRoutingKey(destination);
			}
			else {
				endpoint.setRoutingKeyExpressionString(routingKeyExpression);
			}
		}
		else {
			if (routingKeyExpression == null) {
				endpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(destination, false));
			}
			else {
				endpoint.setRoutingKeyExpressionString(buildPartitionRoutingExpression(routingKeyExpression, true));
			}
		}
		if (extendedProperties.getDelayExpression() != null) {
			endpoint.setDelayExpressionString(extendedProperties.getDelayExpression());
		}
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.outboundMapper();
		mapper.setRequestHeaderNames(extendedProperties.getRequestHeaderPatterns());
		mapper.setReplyHeaderNames(extendedProperties.getReplyHeaderPatterns());
		endpoint.setHeaderMapper(mapper);
		endpoint.setDefaultDeliveryMode(extendedProperties.getDeliveryMode());
		endpoint.setBeanFactory(this.getBeanFactory());
		endpoint.afterPropertiesSet();
		return endpoint;
	}

	private String buildPartitionRoutingExpression(String expressionRoot, boolean rootIsExpression) {
		return rootIsExpression
				? expressionRoot + " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']"
				: "'" + expressionRoot + "-' + headers['" + BinderHeaders.PARTITION_HEADER + "']";
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination consumerDestination, String group,
													ExtendedConsumerProperties<RabbitConsumerProperties> properties) {

		DirectChannel convertingBridgeChannel = new DirectChannel();
		convertingBridgeChannel.setBeanFactory(this.getBeanFactory());

		String prefix = properties.getExtension().getPrefix();
		String destination = consumerDestination.getName();
		String prefixStripped = (StringUtils.isEmpty(prefix) || !destination.startsWith(prefix)) ? destination
				: destination.substring(prefix.length());
		String baseQueueName = StringUtils.hasText(group) ? prefixStripped.substring(0, prefixStripped.indexOf(group)) + group : prefixStripped;

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
		listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(consumerDestination.getName() + "-"));
		listenerContainer.setQueueNames(consumerDestination.getName());
		if (properties.getMaxAttempts() > 1 || properties.getExtension().isRepublishToDlq()) {
			RetryOperationsInterceptor retryInterceptor = RetryInterceptorBuilder.stateless()
					.retryOperations(buildRetryTemplate(properties))
					.recoverer(determineRecoverer(baseQueueName, properties.getExtension(),
							properties.getExtension().isRepublishToDlq()))
					.build();
			listenerContainer.setAdviceChain(retryInterceptor);
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

	private String deadLetterExchangeName(RabbitCommonProperties properties) {
		if (properties.getDeadLetterExchange() == null) {
			return properties.getPrefix() + RabbitCommonProperties.DEAD_LETTER_EXCHANGE;
		}
		else {
			return properties.getDeadLetterExchange();
		}
	}

	@Override
	protected void afterUnbindConsumer(ConsumerDestination consumerDestination, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {
		provisioningProvider.cleanAutoDeclareContext(consumerDestination.getName());
	}

	private MessageRecoverer determineRecoverer(String name, RabbitCommonProperties properties, boolean republish) {
		if (republish) {
			RabbitTemplate errorTemplate = new RabbitTemplate(this.connectionFactory);
			return new RepublishMessageRecoverer(errorTemplate,
					deadLetterExchangeName(properties),
					applyPrefix(properties.getPrefix(), name));
		}
		else {
			return new RejectAndDontRequeueRecoverer();
		}
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

}
