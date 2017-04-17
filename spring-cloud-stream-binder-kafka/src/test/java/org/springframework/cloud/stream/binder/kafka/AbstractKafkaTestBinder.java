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

import java.util.List;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.tuple.TupleKryoRegistrar;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;

/**
 * @author Soby Chacko
 * @author Gary Russell
 */
public abstract class AbstractKafkaTestBinder extends
		AbstractTestBinder<KafkaMessageChannelBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	@Override
	public void cleanup() {
		// do nothing - the rule will take care of that
	}

	protected void addErrorChannel(GenericApplicationContext context) {
		PublishSubscribeChannel errorChannel = new PublishSubscribeChannel();
		context.getBeanFactory().initializeBean(errorChannel, IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME);
		context.getBeanFactory().registerSingleton(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, errorChannel);
	}

	protected static Codec getCodec() {
		return new PojoCodec(new TupleRegistrar());
	}

	private static class TupleRegistrar implements KryoRegistrar {
		private final TupleKryoRegistrar delegate = new TupleKryoRegistrar();

		@Override
		public void registerTypes(Kryo kryo) {
			this.delegate.registerTypes(kryo);
		}

		@Override
		public List<Registration> getRegistrations() {
			return this.delegate.getRegistrations();
		}
	}

}

