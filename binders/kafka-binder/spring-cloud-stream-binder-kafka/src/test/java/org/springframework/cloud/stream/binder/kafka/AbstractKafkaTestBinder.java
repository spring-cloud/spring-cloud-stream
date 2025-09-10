/*
 * Copyright 2014-present the original author or authors.
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

import org.springframework.cloud.stream.binder.AbstractPollableConsumerTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.context.ApplicationContext;

/**
 * @author Soby Chacko
 * @author Gary Russell
 */
abstract class AbstractKafkaTestBinder extends
		// @checkstyle:off
		AbstractPollableConsumerTestBinder<KafkaMessageChannelBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	// @checkstyle:on
	private ApplicationContext applicationContext;

	@Override
	public void cleanup() {
		// do nothing - the rule will take care of that
	}

	protected final void setApplicationContext(ApplicationContext context) {
		this.applicationContext = context;
	}

	public ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

}
