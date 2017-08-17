/*
 * Copyright 2014-2017 the original author or authors.
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
package org.springframework.cloud.stream.binder.kafka;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.tuple.TupleKryoRegistrar;

/**
 * @author Soby Chacko
 * @author Gary Russell
 */
public abstract class AbstractKafkaTestBinder extends
		AbstractTestBinder<KafkaMessageChannelBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

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

	protected static Codec getCodec() {
		return new PojoCodec(new TupleKryoRegistrar());
	}

}

