/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.function;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;

/**
 *
 * @author Oleg Zhurakousky
 * @author Tolga Kavukcu
 *
 * @since 2.1
 */
@ConfigurationProperties("spring.cloud.stream.function")
public class StreamFunctionProperties {

	/**
	 * Definition of functions to bind. If several functions need to be composed
	 * into one, use pipes (e.g., 'fooFunc|barFunc')
	 */
	private String definition;

	private ConsumerProperties consumerProperties;

	private ProducerProperties producerProperties;

	public String getDefinition() {
		return this.definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

	protected ConsumerProperties getConsumerProperties() {
		return consumerProperties;
	}

	void setConsumerProperties(ConsumerProperties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	protected ProducerProperties getProducerProperties() {
		return producerProperties;
	}

	void setProducerProperties(ProducerProperties producerProperties) {
		this.producerProperties = producerProperties;
	}
}
