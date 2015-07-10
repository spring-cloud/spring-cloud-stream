/*
 * Copyright 2014-2015 the original author or authors.
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

package org.springframework.xd.test.kafka;


import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Rule;

import org.springframework.xd.test.AbstractExternalResourceTestSupport;

/**
 * JUnit {@link Rule} that starts an embedded Kafka server (with an associated Zookeeper)
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @since 1.1
 */
public class KafkaTestSupport extends AbstractExternalResourceTestSupport<String> {

	private static final Logger log = LoggerFactory.getLogger(KafkaTestSupport.class);

	private static final String XD_KAFKA_TEST_EMBEDDED = "XD_KAFKA_TEST_EMBEDDED";

	public static final boolean embedded;

	private static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";

	private static final String DEFAULT_KAFKA_CONNECT = "localhost:9092";

	private ZkClient zkClient;

	private EmbeddedZookeeper zookeeper;

	private KafkaServer kafkaServer;

	private Properties brokerConfig = TestUtils.createBrokerConfig(0, TestUtils.choosePort(), false);

	static {
		embedded = "true".equals(System.getProperty(XD_KAFKA_TEST_EMBEDDED));
		log.info(String.format("Testing with %s Kafka broker", embedded ? "embedded" : "external"));
	}

	public KafkaTestSupport() {
		super("KAFKA");
	}

	public String getZkConnectString() {
		if (embedded) {
			return zookeeper.getConnectString();
		}
		else {
			return DEFAULT_ZOOKEEPER_CONNECT;
		}
	}

	public ZkClient getZkClient() {
		return this.zkClient;
	}

	public String getBrokerAddress() {
		if (embedded) {
			return kafkaServer.config().hostName() + ":" + kafkaServer.config().port();
		}
		else {
			return DEFAULT_KAFKA_CONNECT;
		}
	}

	@Override
	protected void obtainResource() throws Exception {
		if (embedded) {
			log.debug("Starting Zookeeper");
			zookeeper = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
			log.debug("Started Zookeeper at " + zookeeper.getConnectString());
			try {
				int zkConnectionTimeout = 6000;
				int zkSessionTimeout = 6000;
				zkClient = new ZkClient(getZkConnectString(), zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer$.MODULE$);
			}
			catch (Exception e) {
				zookeeper.shutdown();
				throw e;
			}
			try {
				log.debug("Creating Kafka server");
				Properties brokerConfigProperties = brokerConfig;
				kafkaServer = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), SystemTime$.MODULE$);
				log.debug("Created Kafka server at " + kafkaServer.config().hostName() + ":" + kafkaServer.config().port());
			}
			catch (Exception e) {
				zookeeper.shutdown();
				zkClient.close();
				throw e;
			}
		}
		else {
			this.zkClient = new ZkClient(DEFAULT_ZOOKEEPER_CONNECT, 5000, 5000, ZKStringSerializer$.MODULE$);
			if (ZkUtils.getAllBrokersInCluster(zkClient).size() == 0) {
				throw new RuntimeException("Kafka server not available");
			}
		}
	}

	@Override
	protected void cleanupResource() throws Exception {
		if (embedded) {
			try {
				kafkaServer.shutdown();
			}
			catch (Exception e) {
				// ignore errors on shutdown
				log.error(e.getMessage(), e);
			}
			try {
				Utils.rm(kafkaServer.config().logDirs());
			}
			catch (Exception e) {
				// ignore errors on shutdown
				log.error(e.getMessage(), e);
			}
		}
		try {
			zkClient.close();
		}
		catch (ZkInterruptedException e) {
			// ignore errors on shutdown
			log.error(e.getMessage(), e);
		}
		if (embedded) {
			try {
				zookeeper.shutdown();
			}
			catch (Exception e) {
				// ignore errors on shutdown
				log.error(e.getMessage(), e);
			}
		}
	}

}
