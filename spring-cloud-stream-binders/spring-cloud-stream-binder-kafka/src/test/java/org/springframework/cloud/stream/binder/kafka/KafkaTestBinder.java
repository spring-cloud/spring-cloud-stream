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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
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
import kafka.admin.AdminUtils;

/**
 * Test support class for {@link KafkaMessageChannelBinder}. Creates a binder that uses a
 * test {@link TestKafkaCluster kafka cluster}.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author David Turanski
 * @author Gary Russell
 * @author Soby Chacko
 * @author Ilayaperumal Gopinathan
 */
public class KafkaTestBinder extends
		AbstractTestBinder<KafkaMessageChannelBinder, ExtendedConsumerProperties<KafkaConsumerProperties>, ExtendedProducerProperties<KafkaProducerProperties>> {

	protected final Log log = LogFactory.getLog(getClass());

	private final KafkaTestSupport kafkaTestSupport;

	public KafkaTestBinder(KafkaTestSupport kafkaTestSupport, KafkaBinderConfigurationProperties binderConfiguration) {
		try {
			ZookeeperConnect zookeeperConnect = new ZookeeperConnect();
			zookeeperConnect.setZkConnect(kafkaTestSupport.getZkConnectString());
			KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(zookeeperConnect,
					kafkaTestSupport.getBrokerAddress(), kafkaTestSupport.getZkConnectString());
			binder.setCodec(getCodec());
			ProducerListener producerListener = new LoggingProducerListener();
			binder.setProducerListener(producerListener);
			GenericApplicationContext context = new GenericApplicationContext();
			context.refresh();
			binder.setApplicationContext(context);
			binder.setFetchSize(binderConfiguration.getFetchSize());
			binder.setMaxWait(binderConfiguration.getMaxWait());
			binder.setMinPartitionCount(binderConfiguration.getMinPartitionCount());
			binder.afterPropertiesSet();
			this.setBinder(binder);
			this.kafkaTestSupport = kafkaTestSupport;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup() {
		try {
			for (String topic: destinations) {
				AdminUtils.deleteTopic(kafkaTestSupport.getZkClient(), topic);
			}
		}
		catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	private static Codec getCodec() {
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
