/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;
import org.springframework.xd.dirt.integration.bus.serializer.kryo.PojoCodec;
import org.springframework.xd.tuple.serializer.kryo.TupleKryoRegistrar;


/**
 * Test support class for {@link KafkaMessageChannelBinder}.
 * Creates a binder that uses a test {@link TestKafkaCluster kafka cluster}.
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author David Turanski
 */
public class KafkaTestBinder extends AbstractTestBinder<KafkaMessageChannelBinder> {

	public KafkaTestBinder(KafkaTestSupport kafkaTestSupport) {
		this(kafkaTestSupport, getCodec(), KafkaMessageChannelBinder.Mode.embeddedHeaders);
	}


	public KafkaTestBinder(KafkaTestSupport kafkaTestSupport, MultiTypeCodec<Object> codec,
			KafkaMessageChannelBinder.Mode mode) {

		try {
			ZookeeperConnect zookeeperConnect = new ZookeeperConnect();
			zookeeperConnect.setZkConnect(kafkaTestSupport.getZkConnectString());
			KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(zookeeperConnect,
					kafkaTestSupport.getBrokerAddress(),
					kafkaTestSupport.getZkConnectString(), codec);
			binder.setDefaultBatchingEnabled(false);
			binder.setMode(mode);
			binder.afterPropertiesSet();
			GenericApplicationContext context = new GenericApplicationContext();
			context.refresh();
			binder.setApplicationContext(context);
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

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static MultiTypeCodec<Object> getCodec() {
		return new PojoCodec(new TupleKryoRegistrar());
	}

}
