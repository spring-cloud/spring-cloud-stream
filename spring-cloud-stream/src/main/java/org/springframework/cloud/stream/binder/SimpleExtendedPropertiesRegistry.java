/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.BeanCreationException;

/**
 * @author Marius Bogoevici
 */
public class SimpleExtendedPropertiesRegistry<C, P> implements ExtendedPropertiesRegistry<C, P> {

	private final Class<C> consumerPropertiesClass;

	private final Class<P> producerPropertiesClass;

	public SimpleExtendedPropertiesRegistry(Class<C> consumerPropertiesClass, Class<P> producerPropertiesClass) {
		this.consumerPropertiesClass = consumerPropertiesClass;
		this.producerPropertiesClass = producerPropertiesClass;
	}

	private Map<String, C> consumer = new HashMap<>();

	private Map<String, P> producer = new HashMap<>();

	public Map<String, C> getConsumer() {
		return consumer;
	}

	public void setConsumer(Map<String, C> consumer) {
		this.consumer = consumer;
	}

	public Map<String, P> getProducer() {
		return producer;
	}

	public void setProducer(Map<String, P> producer) {
		this.producer = producer;
	}

	public C getExtendedConsumerProperties(String channelName) {
		try {
			return this.consumer.containsKey(channelName) ? this.consumer.get(channelName) : consumerPropertiesClass.newInstance();
		}
		catch (Exception e) {
			throw new BeanCreationException("Cannot create properties instance for: " + consumerPropertiesClass.getName());
		}
	}

	public P getExtendedProducerProperties(String channelName) {
		try {
			return this.producer.containsKey(channelName) ? this.producer.get(channelName) : producerPropertiesClass.newInstance();
		}
		catch (Exception e) {
			throw new BeanCreationException("Cannot create properties instance for: " + producerPropertiesClass.getName());
		}

	}
}
