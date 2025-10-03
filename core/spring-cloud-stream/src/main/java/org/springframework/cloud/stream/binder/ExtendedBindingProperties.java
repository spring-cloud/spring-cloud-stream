/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collections;
import java.util.Map;

/**
 * Properties that extend the common binding properties for a particular binder
 * implementation.
 *
 * @param <C> consumer properties type
 * @param <P> producer properties type
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Soby Chacko
 */
public interface ExtendedBindingProperties<C, P> {

	C getExtendedConsumerProperties(String channelName);

	P getExtendedProducerProperties(String channelName);

	default Map<String, ? extends Object> getBindings() {
		return Collections.emptyMap();
	}

	/**
	 * Extended binding properties can define a default prefix to place all the extended
	 * common producer and consumer properties. For example, if the binder type is foo it
	 * is convenient to specify common extended properties for the producer or consumer
	 * across multiple bindings in the form of
	 * `spring.cloud.stream.foo.default.producer.x=y` or
	 * `spring.cloud.stream.foo.default.consumer.x=y`.
	 *
	 * The binding process will use this defaults prefix to resolve any common extended
	 * producer and consumer properties.
	 * @return default prefix for extended properties
	 * @since 2.1.0
	 */
	String getDefaultsPrefix();

	/**
	 *
	 * Extended properties class which should be a subclass of
	 * {@link BinderSpecificPropertiesProvider} against which default extended producer
	 * and consumer properties are resolved.
	 * @return extended properties class that contains extended producer/consumer
	 * properties
	 * @since 2.1.0
	 */
	Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass();

}
