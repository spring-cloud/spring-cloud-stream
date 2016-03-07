/*
 * Copyright 2015-2016 the original author or authors.
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
import org.springframework.cloud.stream.test.junit.kafka.KafkaTestSupport;
import org.springframework.cloud.stream.test.junit.kafka.TestKafkaCluster;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.kafka.support.LoggingProducerListener;
import org.springframework.integration.kafka.support.ProducerListener;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.integration.tuple.TupleKryoRegistrar;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;


/**
 * Test support class for {@link KafkaMessageChannelBinder}.
 * Creates a binder that uses a test {@link TestKafkaCluster kafka cluster}.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author David Turanski
 * @author Gary Russell
 * @author Soby Chacko
 */
public class KafkaTestBinder extends AbstractTestBinder<KafkaMessageChannelBinder, KafkaConsumerProperties, KafkaProducerProperties> {

	public KafkaTestBinder(KafkaTestSupport kafkaTestSupport) {

		try {
			ZookeeperConnect zookeeperConnect = new ZookeeperConnect();
			zookeeperConnect.setZkConnect(kafkaTestSupport.getZkConnectString());
			KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(zookeeperConnect,
					kafkaTestSupport.getBrokerAddress(),
					kafkaTestSupport.getZkConnectString());
			binder.setCodec(getCodec());
			ProducerListener producerListener = new LoggingProducerListener();
			binder.setProducerListener(producerListener);
			GenericApplicationContext context = new GenericApplicationContext();
			context.refresh();
			binder.setApplicationContext(context);
			binder.afterPropertiesSet();
			this.setBinder(binder);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup() {
		// do nothing - the rule will take care of that
	}

	private static Codec getCodec() {
		return new PojoCodec(new TupleRegistrar());
	}

	private static class TupleRegistrar implements KryoRegistrar {
		private final TupleKryoRegistrar delegate = new TupleKryoRegistrar();

		@Override
		public void registerTypes(Kryo kryo) {
			delegate.registerTypes(kryo);
		}

		@Override
		public List<Registration> getRegistrations() {
			return delegate.getRegistrations();
		}
	}

}
