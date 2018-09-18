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

package org.springframework.cloud.stream.utils;

/**
 * @author Soby Chacko
 */
public class FooBindingProperties {

	private FooExtendedProducerProperties producer = new FooExtendedProducerProperties();

	private FooExtendedConsumerProperties consumer = new FooExtendedConsumerProperties();

	public FooExtendedProducerProperties getProducer() {
		return producer;
	}

	public void setProducer(FooExtendedProducerProperties producer) {
		this.producer = producer;
	}

	public FooExtendedConsumerProperties getConsumer() {
		return consumer;
	}

	public void setConsumer(FooExtendedConsumerProperties consumer) {
		this.consumer = consumer;
	}
}
