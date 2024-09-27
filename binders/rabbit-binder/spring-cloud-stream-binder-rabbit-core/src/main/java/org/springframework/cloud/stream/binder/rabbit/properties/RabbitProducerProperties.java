/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.properties;

import java.util.Optional;

import jakarta.validation.constraints.Min;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;



/**
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class RabbitProducerProperties extends RabbitCommonProperties {

	/**
	 * Determines the producer type.
	 * @since 3.2
	 */
	public enum ProducerType {

		/**
		 * RabbitMQ Stream producer - blocks until confirm received.
		 */
		STREAM_SYNC,

		/**
		 * RabbitMQ Stream producer - does not block.
		 */
		STREAM_ASYNC,

		/**
		 * Classic AMQP producer.
		 */
		AMQP
	}

	/**
	 * true to compress messages.
	 */
	private boolean compress;

	/**
	 * true to batch multiple messages into one.
	 */
	private boolean batchingEnabled;

	/**
	 * the number of messages to batch, when enabled.
	 */
	private int batchSize = 100;

	/**
	 * the size limit for batched messages.
	 */
	private int batchBufferLimit = 10000;

	/**
	 * the time after which an incomplete batch will be sent.
	 */
	private int batchTimeout = 5000;

	/**
	 * the bean name of a custom batching strategy to use instead of the
	 * {@link org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy}.
	 */
	private String batchingStrategyBeanName;

	/**
	 * true to use transacted channels.
	 */
	private boolean transacted;

	/**
	 * the delivery mode for published messages.
	 */
	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	/**
	 * patterns to match which headers are mapped (inbound).
	 */
	private String[] headerPatterns = new String[] { "*" };

	/**
	 * when using a delayed message exchange, a SpEL expression to determine the delay to
	 * apply to messages.
	 */
	private Expression delayExpression;

	/**
	 * a static routing key when publishing messages; default is the destination name;
	 * suffixed by "-partition" when partitioned. This is only used if `routingKeyExpression` is null
	 */
	private String routingKey;

	/**
	 * a custom routing key when publishing messages; default is the destination name;
	 * suffixed by "-partition" when partitioned.
	 */
	private Expression routingKeyExpression;

	/**
	 * the channel name to which to send publisher confirms (acks) if the connection
	 * factory is so configured; default 'nullChannel'; requires
	 * 'errorChannelEnabled=true'.
	 */
	private String confirmAckChannel;

	/**
	 * When true, the binding will complete the {@link java.util.concurrent.Future} field
	 * in a {@link org.springframework.amqp.rabbit.connection.CorrelationData} contained
	 * in the
	 * {@link org.springframework.amqp.support.AmqpHeaders#PUBLISH_CONFIRM_CORRELATION}
	 * header when the confirmation is received.
	 */
	private boolean useConfirmHeader;

	/**
	 * When STREAM_SYNC or STREAM_ASYNC, create a RabbitMQ Stream producer instead of an
	 * AMQP producer.
	 * @since 3.2
	 */
	private ProducerType producerType = ProducerType.AMQP;

	/**
	 * The bean name of a message converter to convert from spring-messaging Message to
	 * a Spring AMQP Message.
	 * @since 3.2
	 */
	private String streamMessageConverterBeanName;

	/**
	 * Configure an alternate exchange for when no queues are bound.
	 * @since 4.0
	 */
	private AlternateExchange alternateExchange;

	/**
	 * When the producer type is STREAM_*, set this to true to publish to a super stream.
	 * Also requires a partition key.
	 */
	private boolean superStream;

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public boolean isCompress() {
		return compress;
	}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return deliveryMode;
	}

	public String[] getHeaderPatterns() {
		return headerPatterns;
	}

	public void setHeaderPatterns(String[] replyHeaderPatterns) {
		this.headerPatterns = replyHeaderPatterns;
	}

	public boolean isBatchingEnabled() {
		return batchingEnabled;
	}

	public void setBatchingEnabled(boolean batchingEnabled) {
		this.batchingEnabled = batchingEnabled;
	}

	@Min(value = 1, message = "Batch Size should be greater than zero.")
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	@Min(value = 1, message = "Batch Buffer Limit should be greater than zero.")
	public int getBatchBufferLimit() {
		return batchBufferLimit;
	}

	public void setBatchBufferLimit(int batchBufferLimit) {
		this.batchBufferLimit = batchBufferLimit;
	}

	@Min(value = 1, message = "Batch Timeout should be greater than zero.")
	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public boolean isTransacted() {
		return this.transacted;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public Expression getDelayExpression() {
		return this.delayExpression;
	}

	public void setDelayExpression(Expression delayExpression) {
		this.delayExpression = delayExpression;
	}

	public Expression getRoutingKeyExpression() {
		return Optional.ofNullable(this.routingKeyExpression)
				.orElseGet(() -> Optional.ofNullable(this.routingKey)
						.map(LiteralExpression::new)
						.orElse(null));
	}

	public void setRoutingKeyExpression(Expression routingKeyExpression) {
		this.routingKeyExpression = routingKeyExpression;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public String getConfirmAckChannel() {
		return this.confirmAckChannel;
	}

	public void setConfirmAckChannel(String confirmAckChannel) {
		this.confirmAckChannel = confirmAckChannel;
	}

	public String getBatchingStrategyBeanName() {
		return batchingStrategyBeanName;
	}

	public void setBatchingStrategyBeanName(String batchingStrategyBeanName) {
		this.batchingStrategyBeanName = batchingStrategyBeanName;
	}

	public boolean isUseConfirmHeader() {
		return this.useConfirmHeader;
	}

	public void setUseConfirmHeader(boolean useConfirmHeader) {
		this.useConfirmHeader = useConfirmHeader;
	}

	public ProducerType getProducerType() {
		return this.producerType;
	}

	public void setProducerType(ProducerType producerType) {
		Assert.notNull(producerType, "'producerType' cannot be null");
		this.producerType = producerType;
	}

	public String getStreamMessageConverterBeanName() {
		return this.streamMessageConverterBeanName;
	}

	public void setStreamMessageConverterBeanName(String streamMessageConverterBeanName) {
		this.streamMessageConverterBeanName = streamMessageConverterBeanName;
	}

	@Nullable
	public AlternateExchange getAlternateExchange() {
		return this.alternateExchange;
	}

	public void setAlternateExchange(AlternateExchange alternate) {
		this.alternateExchange = alternate;
	}

	public boolean isSuperStream() {
		return this.superStream;
	}

	public void setSuperStream(boolean superStream) {
		this.superStream = superStream;
	}

	public static class AlternateExchange {

		/**
		 * The alternate exchange name.
		 */
		private String name;

		/**
		 * Whether the exchange exists or should be provisioned.
		 */
		private boolean exists = false;

		/**
		 * The alternate exchange type.
		 */
		private String type = ExchangeTypes.TOPIC;

		/**
		 * Bind a durable queue to the alternate exchange.
		 */
		private Binding binding;

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public boolean isExists() {
			return this.exists;
		}

		public void setExists(boolean exists) {
			this.exists = exists;
		}

		public String getType() {
			return this.type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public Binding getBinding() {
			return this.binding;
		}

		public void setBinding(Binding binding) {
			this.binding = binding;
		}

		public static class Binding {

			/**
			 * The routing key.
			 */
			private String routingKey = "#";

			/**
			 * The queue name.
			 */
			private String queue;

			public String getRoutingKey() {
				return this.routingKey;
			}

			public void setRoutingKey(String routingKey) {
				this.routingKey = routingKey;
			}

			public String getQueue() {
				return this.queue;
			}

			public void setQueue(String queue) {
				this.queue = queue;
			}


		}

	}

}
