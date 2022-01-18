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

package org.springframework.cloud.stream.binding;

import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.messaging.MessageChannel;

/**
 * Configure a new destination before it is bound.
 *
 * @param <T> the extended properties type. If you need to support dynamic binding
 * with multiple binders, use {@link Object} and cast as needed.
 * @since 2.0
 *
 */
@FunctionalInterface
public interface NewDestinationBindingCallback<T> {

	/**
	 * Configure the properties or channel before binding.
	 * @param channelName the name of the new channel.
	 * @param channel the channel that is about to be bound.
	 * @param producerProperties the producer properties.
	 * @param extendedProducerProperties the extended producer properties (type
	 * depends on binder type and may be null if the binder doesn't support extended
	 * properties).
	 */
	void configure(String channelName, MessageChannel channel,
				ProducerProperties producerProperties, T extendedProducerProperties);

}
