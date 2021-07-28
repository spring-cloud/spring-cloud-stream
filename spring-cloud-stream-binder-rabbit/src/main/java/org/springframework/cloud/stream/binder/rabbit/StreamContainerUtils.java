/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.MessageBuilder.ApplicationPropertiesBuilder;
import com.rabbitmq.stream.MessageBuilder.PropertiesBuilder;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.codec.WrapperMessageBuilder;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

/**
 * Utilities for stream containers. Used to prevent a hard runtime dependency on
 * spring-rabbit-stream.
 *
 * @author Gary Russell
 * @since 3.2
 *
 */
public final class StreamContainerUtils {

	private StreamContainerUtils() {
	}

	/**
	 * Create a {@link StreamListenerContainer}.
	 *
	 * @param consumerDestination the destination.
	 * @param group the group.
	 * @param properties the properties.
	 * @param destination the destination.
	 * @param extension the properties extension.
	 * @param applicationContext the application context.
	 * @return the container.
	 */
	public static MessageListenerContainer createContainer(ConsumerDestination consumerDestination, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties, String destination,
			RabbitConsumerProperties extension, AbstractApplicationContext applicationContext) {

		StreamListenerContainer container = new StreamListenerContainer(applicationContext.getBean(Environment.class)) {

			@Override
			public synchronized void setConsumerCustomizer(Consumer<ConsumerBuilder> consumerCustomizer) {
				super.setConsumerCustomizer(builder -> {
					builder.name(consumerDestination.getName());
					consumerCustomizer.accept(builder);
				});
			}


		};
		container.setMessageConverter(new DefaultStreamMessageConverter());
		return container;
	}

	/**
	 * Configure the channel adapter for streams support.
	 * @param adapter the adapter.
	 */
	public static void configureAdapter(AmqpInboundChannelAdapter adapter) {
		adapter.setHeaderMapper(new AmqpHeaderMapper() {

			AmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.inboundMapper();

			@Override
			public Map<String, Object> toHeadersFromRequest(MessageProperties source) {
				Map<String, Object> headers = this.mapper.toHeadersFromRequest(source);
				headers.put("rabbitmq_streamContext", ((StreamMessageProperties) source).getContext());
				return headers;
			}

			@Override
			public Map<String, Object> toHeadersFromReply(MessageProperties source) {
				return null;
			}

			@Override
			public void fromHeadersToRequest(MessageHeaders headers, MessageProperties target) {
			}

			@Override
			public void fromHeadersToReply(MessageHeaders headers, MessageProperties target) {
			}

		});
	}

}

/**
 * Temporary work-around for a bug in spring-rabbit-stream 2.4.0-M1.
 */
class DefaultStreamMessageConverter implements StreamMessageConverter {

	private final Supplier<MessageBuilder> builderSupplier;

	private final Charset charset = StandardCharsets.UTF_8;

	/**
	 * Construct an instance using a {@link WrapperMessageBuilder}.
	 */
	DefaultStreamMessageConverter() {
		this.builderSupplier = () -> new WrapperMessageBuilder();
	}

	/**
	 * Construct an instance using the provided codec.
	 * @param codec the codec.
	 */
	DefaultStreamMessageConverter(@Nullable Codec codec) {
		this.builderSupplier = () -> codec.messageBuilder();
	}

	@Override
	public Message toMessage(Object object, StreamMessageProperties messageProperties) throws MessageConversionException {
		Assert.isInstanceOf(com.rabbitmq.stream.Message.class, object);
		com.rabbitmq.stream.Message streamMessage = (com.rabbitmq.stream.Message) object;
		toMessageProperties(streamMessage, messageProperties);
		return org.springframework.amqp.core.MessageBuilder.withBody(streamMessage.getBodyAsBinary())
				.andProperties(messageProperties)
				.build();
	}

	@Override
	public com.rabbitmq.stream.Message fromMessage(Message message) throws MessageConversionException {
		MessageBuilder builder = this.builderSupplier.get();
		PropertiesBuilder propsBuilder = builder.properties();
		MessageProperties props = message.getMessageProperties();
		Assert.isInstanceOf(StreamMessageProperties.class, props);
		StreamMessageProperties mProps = (StreamMessageProperties) props;
		JavaUtils.INSTANCE
				.acceptIfNotNull(mProps.getMessageId(), propsBuilder::messageId) // TODO different types
				.acceptIfNotNull(mProps.getUserId(), usr -> propsBuilder.userId(usr.getBytes(this.charset)))
				.acceptIfNotNull(mProps.getTo(), propsBuilder::to)
				.acceptIfNotNull(mProps.getSubject(), propsBuilder::subject)
				.acceptIfNotNull(mProps.getReplyTo(), propsBuilder::replyTo)
				.acceptIfNotNull(mProps.getCorrelationId(), propsBuilder::correlationId) // TODO different types
				.acceptIfNotNull(mProps.getContentType(), propsBuilder::contentType)
				.acceptIfNotNull(mProps.getContentEncoding(), propsBuilder::contentEncoding)
				.acceptIfNotNull(mProps.getExpiration(), exp -> propsBuilder.absoluteExpiryTime(Long.parseLong(exp)))
				.acceptIfNotNull(mProps.getCreationTime(), propsBuilder::creationTime)
				.acceptIfNotNull(mProps.getGroupId(), propsBuilder::groupId)
				.acceptIfNotNull(mProps.getGroupSequence(), propsBuilder::groupSequence)
				.acceptIfNotNull(mProps.getReplyToGroupId(), propsBuilder::replyToGroupId);
		if (mProps.getHeaders().size() > 0) {
			ApplicationPropertiesBuilder appPropsBuilder = builder.applicationProperties();
			mProps.getHeaders().forEach((key, val) -> {
				mapProp(key, val, appPropsBuilder);
			});
		}
		builder.addData(message.getBody());
		return builder.build();
	}

	private void mapProp(String key, Object val, ApplicationPropertiesBuilder builder) { // NOSONAR - complexity
		if (val instanceof String) {
			builder.entry(key, (String) val);
		}
		else if (val instanceof Long) {
			builder.entry(key, (Long) val);
		}
		else if (val instanceof Integer) {
			builder.entry(key, (Integer) val);
		}
		else if (val instanceof Short) {
			builder.entry(key, (Short) val);
		}
		else if (val instanceof Byte) {
			builder.entry(key, (Byte) val);
		}
		else if (val instanceof Double) {
			builder.entry(key, (Double) val);
		}
		else if (val instanceof Float) {
			builder.entry(key, (Float) val);
		}
		else if (val instanceof Character) {
			builder.entry(key, (Character) val);
		}
		else if (val instanceof UUID) {
			builder.entry(key, (UUID) val);
		}
		else if (val instanceof byte[]) {
			builder.entry(key, (byte[]) val);
		}
	}

	private void toMessageProperties(com.rabbitmq.stream.Message streamMessage,
			StreamMessageProperties mProps) {

		Properties properties = streamMessage.getProperties();
		if (properties != null) {
			JavaUtils.INSTANCE
					.acceptIfNotNull(properties.getMessageIdAsString(), mProps::setMessageId)
					.acceptIfNotNull(properties.getUserId(), usr -> mProps.setUserId(new String(usr, this.charset)))
					.acceptIfNotNull(properties.getTo(), mProps::setTo)
					.acceptIfNotNull(properties.getSubject(), mProps::setSubject)
					.acceptIfNotNull(properties.getReplyTo(), mProps::setReplyTo)
					.acceptIfNotNull(properties.getCorrelationIdAsString(), mProps::setCorrelationId)
					.acceptIfNotNull(properties.getContentType(), mProps::setContentType)
					.acceptIfNotNull(properties.getContentEncoding(), mProps::setContentEncoding)
					.acceptIfNotNull(properties.getAbsoluteExpiryTime(),
							exp -> mProps.setExpiration(Long.toString(exp)))
					.acceptIfNotNull(properties.getCreationTime(), mProps::setCreationTime)
					.acceptIfNotNull(properties.getGroupId(), mProps::setGroupId)
					.acceptIfNotNull(properties.getGroupSequence(), mProps::setGroupSequence)
					.acceptIfNotNull(properties.getReplyToGroupId(), mProps::setReplyToGroupId);
		}
		Map<String, Object> applicationProperties = streamMessage.getApplicationProperties();
		if (applicationProperties != null) {
			mProps.getHeaders().putAll(applicationProperties);
		}
	}

}
