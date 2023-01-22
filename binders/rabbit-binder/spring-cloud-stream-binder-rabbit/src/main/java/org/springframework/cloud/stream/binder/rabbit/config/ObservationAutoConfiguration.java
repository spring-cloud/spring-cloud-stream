/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.messaging.MessageHandler;

@Configuration(proxyBeanMethods = false)
@ConditionalOnBean(org.springframework.boot.actuate.autoconfigure.observation.ObservationAutoConfiguration.class)
public class ObservationAutoConfiguration {

	@Bean
	@Order(Ordered.HIGHEST_PRECEDENCE)
	ListenerContainerCustomizer<MessageListenerContainer> observedListenerContainerCustomizer(
			ApplicationContext applicationContext) {
		return (container, destinationName, group) -> {
			if (container instanceof AbstractMessageListenerContainer abstractMessageListenerContainer) {
				abstractMessageListenerContainer.setObservationEnabled(true);
				abstractMessageListenerContainer.setApplicationContext(applicationContext);
			}
		};
	}

	@Bean
	@Order(Ordered.HIGHEST_PRECEDENCE)
	ProducerMessageHandlerCustomizer<MessageHandler> observedProducerMessageHandlerCustomizer(
			ApplicationContext applicationContext) {
		return (handler, destinationName) -> {
			if (handler instanceof AmqpOutboundEndpoint amqpOutboundEndpoint) {
				amqpOutboundEndpoint.getRabbitTemplate().setObservationEnabled(true);
				amqpOutboundEndpoint.getRabbitTemplate().setApplicationContext(applicationContext);
			}
		};
	}
}
