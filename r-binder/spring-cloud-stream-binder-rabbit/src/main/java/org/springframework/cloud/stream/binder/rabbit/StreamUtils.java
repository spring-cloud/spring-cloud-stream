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

import java.util.Map;
import java.util.function.Function;

import com.rabbitmq.stream.Environment;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties.ProducerType;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.rabbit.stream.listener.ConsumerCustomizer;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;

/**
 * Utilities for stream components. Used to prevent a hard runtime dependency on
 * spring-rabbit-stream.
 *
 * @author Gary Russell
 * @since 3.2
 *
 */
public final class StreamUtils {

	private StreamUtils() {
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
			public synchronized void setConsumerCustomizer(ConsumerCustomizer consumerCustomizer) {
				super.setConsumerCustomizer((id, builder) -> {
					builder.name(consumerDestination.getName() + "." + group);
					consumerCustomizer.accept(id, builder);
				});
			}


		};
		container.setBeanName(consumerDestination.getName() + "." + group + ".container");
		String beanName = extension.getStreamStreamMessageConverterBeanName();
		if (beanName != null) {
			container.setStreamConverter(applicationContext.getBean(beanName, StreamMessageConverter.class));
		}
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

	/**
	 * Create a {@link RabbitStreamMessageHandler}.
	 *
	 * @param producerDestination the destination.
	 * @param producerProperties the properties.
	 * @param errorChannel the error channel
	 * @param destination the destination.
	 * @param extendedProperties the extended properties.
	 * @param abstractApplicationContext the application context.
	 * @param headerMapperFunction the header mapper function.
	 * @return the handler.
	 */
	public static MessageHandler createStreamMessageHandler(ProducerDestination producerDestination,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties, MessageChannel errorChannel,
			String destination, RabbitProducerProperties extendedProperties,
			AbstractApplicationContext applicationContext,
			Function<RabbitProducerProperties, AmqpHeaderMapper> headerMapperFunction) {

		RabbitStreamTemplate template = new RabbitStreamTemplate(applicationContext.getBean(Environment.class),
				producerDestination.getName());
		String beanName = extendedProperties.getStreamMessageConverterBeanName();
		if (beanName != null) {
			template.setMessageConverter(applicationContext.getBean(beanName, MessageConverter.class));
		}
		beanName = extendedProperties.getStreamStreamMessageConverterBeanName();
		if (beanName != null) {
			template.setStreamConverter(applicationContext.getBean(beanName, StreamMessageConverter.class));
		}
		RabbitStreamMessageHandler handler = new RabbitStreamMessageHandler(template);
		if (errorChannel != null) {
			handler.setFailureCallback((msg, ex) -> {
				errorChannel.send(new ErrorMessage(new MessageHandlingException(msg, ex)));
			});
		}
		handler.setHeaderMapper(headerMapperFunction.apply(extendedProperties));
		handler.setSync(ProducerType.STREAM_SYNC.equals(producerProperties.getExtension().getProducerType()));
		return handler;
	}

}

