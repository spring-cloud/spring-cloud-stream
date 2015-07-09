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

package org.springframework.xd.dirt.integration.kafka;

import kafka.admin.AdminUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.TestingServer;

import org.springframework.util.Assert;
import org.springframework.util.SocketUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;


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
		scala.collection.Iterator<Properties> propsI =
				TestUtils.createBrokerConfigs(1, false).iterator();
		assert propsI.hasNext();
		Properties props = propsI.next();
		assert props.containsKey("zookeeper.connect");
		props.put("zookeeper.connect", zkConnectString);
		return new KafkaConfig(props);
	}

	public String getKafkaBrokerString() {
		return String.format("localhost:%d",
				kafkaServer.serverConfig().port());
	}

	public void stop() throws IOException {
		kafkaServer.shutdown();
		zkServer.stop();
	}


	/**
	 * See XD-2293. This is used to reproduce Kafka rebalance issues.
	 */
	public static void main(String[] args) throws Exception {
		TestKafkaCluster cluster = new TestKafkaCluster();
		ZkClient client = new ZkClient(cluster.getZkConnectString(), 10000, 10000, KafkaMessageBus.utf8Serializer);
		int partitions = 5;
		int replication = 1;
		AdminUtils.createTopic(client, "mytopic", partitions, replication, new Properties());

		Properties props = new Properties();
		props.put("zookeeper.connect", cluster.getZkConnectString());
		props.put("group.id", "foo");
		props.put("rebalance.backoff.ms", "2000");
		props.put("rebalance.max.retries", "2000");
		ConsumerConfig config = new ConsumerConfig(props);


		CuratorFramework curator = CuratorFrameworkFactory.newClient(cluster.getZkConnectString(), new RetryUntilElapsed(1000, 100));
		curator.start();

		RebalanceListener listener = null;
		for (int i = 0; i < 5; i++) {
			System.out.format("%nCreating consumer #%d%n", i + 1);
			ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
			connector.createMessageStreams(Collections.singletonMap("mytopic", 1));
			if (i == 0) {
				PathChildrenCache cache = new PathChildrenCache(curator, "/consumers/foo/owners/mytopic", true);
				listener = new RebalanceListener(5);
				cache.getListenable().addListener(listener);
				cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
			}

			synchronized (listener) {
				System.out.println("******** Waiting for rebalance...");
				listener.wait();
			}

		}

		System.out.println();

	}

	public String getZkConnectString() {
		return zkServer.getConnectString();
	}

	private static class RebalanceListener implements PathChildrenCacheListener {

		private int expected;

		private int actual;

		private boolean ready;

		public RebalanceListener(int expected) {
			this.expected = expected;
		}

		@Override
		public synchronized void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			System.out.println(event);
			System.out.println(event.getData() != null ? new String(event.getData().getData()) : "no data");
			switch (event.getType()) {
				case CHILD_ADDED:
					actual++;
					if (ready && actual == expected) {
						System.out.println("*** Moving on... ");
						this.notify();
					}
					break;
				case CHILD_REMOVED:
					actual--;
					break;
				case INITIALIZED:
					Assert.isTrue(actual == expected);
					ready = true;
					this.notify();
					break;
			}
		}
	}
}
