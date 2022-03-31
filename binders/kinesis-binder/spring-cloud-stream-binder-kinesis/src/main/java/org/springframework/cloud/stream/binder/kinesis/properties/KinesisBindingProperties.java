/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis.properties;

import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

/**
 * The Kinesis-specific binding configuration properties.
 *
 * @author Peter Oates
 * @author Artem Bilan
 */
public class KinesisBindingProperties implements BinderSpecificPropertiesProvider {

	private KinesisConsumerProperties consumer = new KinesisConsumerProperties();

	private KinesisProducerProperties producer = new KinesisProducerProperties();

	public KinesisConsumerProperties getConsumer() {
		return this.consumer;
	}

	public void setConsumer(KinesisConsumerProperties consumer) {
		this.consumer = consumer;
	}

	public KinesisProducerProperties getProducer() {
		return this.producer;
	}

	public void setProducer(KinesisProducerProperties producer) {
		this.producer = producer;
	}

}
