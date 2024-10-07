/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

/**
 * Extension of {@link ListenerContainerCustomizer} specific to Kafka binder.
 * This interface allows for customization of Kafka listener containers with
 * access to Kafka-specific extended consumer properties.
 *
 * @author Soby Chacko
 * @since 4.2.0
 */
public interface KafkaListenerContainerCustomizer extends ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> {

	/**
	 * Configure the Kafka listener container with access to extended consumer properties.
	 *
	 * @param container the Kafka message listener container to configure
	 * @param destinationName the name of the destination (topic) that this listener container is associated with
	 * @param group the consumer group name
	 * @param extendedConsumerProperties the extended consumer properties specific to Kafka
	 */
	default void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String group,
						ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
		configure(container, destinationName, group);
	}
}
