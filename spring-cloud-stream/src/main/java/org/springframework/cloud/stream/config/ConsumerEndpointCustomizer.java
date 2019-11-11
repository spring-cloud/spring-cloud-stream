/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.config;

import org.springframework.integration.core.MessageProducer;

/**
 * If a single bean of this type is in the application context, an inbound channel adapter
 * created by the binder can be further customized after all the properties are set. For
 * example, to configure less-common properties.
 *
 * @param <E> {@link MessageProducer} type
 *
 * @author Gary Russell
 *
 * @since 3.0
 */
@FunctionalInterface
public interface ConsumerEndpointCustomizer<E extends MessageProducer> {

	/**
	 * Configure a {@link MessageProducer} that is being created by the binder for the
	 * provided destination name and group.
	 * @param endpoint the {@link MessageProducer} from the binder.
	 * @param destinationName the bound destination name.
	 * @param group the group.
	 */
	void configure(E endpoint, String destinationName, String group);

}
