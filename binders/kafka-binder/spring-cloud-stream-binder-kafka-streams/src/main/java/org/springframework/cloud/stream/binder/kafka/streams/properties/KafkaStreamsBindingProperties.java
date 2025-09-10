/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.properties;

import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * Extended binding properties holder that delegates to Kafka Streams producer and
 * consumer properties.
 *
 * @author Marius Bogoevici
 */
public class KafkaStreamsBindingProperties implements BinderSpecificPropertiesProvider {

	private KafkaStreamsConsumerProperties consumer = new KafkaStreamsConsumerProperties();

	private KafkaStreamsProducerProperties producer = new KafkaStreamsProducerProperties();

	public KafkaStreamsConsumerProperties getConsumer() {
		return this.consumer;
	}

	public void setConsumer(KafkaStreamsConsumerProperties consumer) {
		this.consumer = consumer;
	}

	public KafkaStreamsProducerProperties getProducer() {
		return this.producer;
	}

	public void setProducer(KafkaStreamsProducerProperties producer) {
		this.producer = producer;
	}

}
