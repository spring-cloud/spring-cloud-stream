/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.cloud.stream.test.junit.kafka;

import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import org.apache.curator.test.TestingServer;

import org.springframework.util.SocketUtils;

/**
 * A test Kafka + ZooKeeper pair for testing purposes.
 *
 * @author Eric Bottard
 */
public class TestKafkaCluster {

	private KafkaServerStartable kafkaServer;

	private TestingServer zkServer;

	public TestKafkaCluster() {
		try {
			zkServer = new TestingServer(SocketUtils.findAvailableTcpPort());
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		KafkaConfig config = getKafkaConfig(zkServer.getConnectString());
		kafkaServer = new KafkaServerStartable(config);
		kafkaServer.startup();
	}

	private static KafkaConfig getKafkaConfig(final String zkConnectString) {
		scala.collection.Iterator<Properties> propsI = TestUtils
				.createBrokerConfigs(1, false).iterator();
		assert propsI.hasNext();
		Properties props = propsI.next();
		assert props.containsKey("zookeeper.connect");
		props.put("zookeeper.connect", zkConnectString);
		return new KafkaConfig(props);
	}

	public String getKafkaBrokerString() {
		return String.format("localhost:%d", kafkaServer.serverConfig().port());
	}

	public void stop() throws IOException {
		kafkaServer.shutdown();
		zkServer.stop();
	}

	public String getZkConnectString() {
		return zkServer.getConnectString();
	}

}
