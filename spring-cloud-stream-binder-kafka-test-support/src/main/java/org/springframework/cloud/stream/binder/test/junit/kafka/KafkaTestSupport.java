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

package org.springframework.cloud.stream.binder.test.junit.kafka;


import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;

import org.springframework.cloud.stream.test.junit.AbstractExternalResourceTestSupport;
import org.springframework.util.SocketUtils;


/**
 * JUnit {@link Rule} that starts an embedded Kafka server (with an associated Zookeeper)
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 * @since 1.1
 */
public class KafkaTestSupport extends AbstractExternalResourceTestSupport<String> {

	private static final Log log = LogFactory.getLog(KafkaTestSupport.class);

	private static final String SCS_KAFKA_TEST_EMBEDDED = "SCS_KAFKA_TEST_EMBEDDED";

	public static final boolean defaultEmbedded;

	private static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";

	private static final String DEFAULT_KAFKA_CONNECT = "localhost:9092";

	private ZkClient zkClient;

	private EmbeddedZookeeper zookeeper;

	private KafkaServer kafkaServer;

	public final boolean embedded;

	private final Properties brokerConfig = TestUtils.createBrokerConfig(0, TestUtils.choosePort(), false);

	// caches previous failures to reach the external server - preventing repeated retries
	private static boolean hasFailedAlready;

	static {
		// check if either the environment or Java property is set to use embedded tests
		// unless the property is explicitly set to false, default to embedded
		defaultEmbedded = !("false".equals(System.getenv(SCS_KAFKA_TEST_EMBEDDED))
				|| "false".equals(System.getProperty(SCS_KAFKA_TEST_EMBEDDED)));
	}

	public KafkaTestSupport() {
		this(defaultEmbedded);
	}

	public KafkaTestSupport(boolean embedded) {
		super("KAFKA");
		this.embedded = embedded;
		log.info(String.format("Testing with %s Kafka broker", embedded ? "embedded" : "external"));
	}

	public KafkaServer getKafkaServer() {
		return this.kafkaServer;
	}

	public String getZkConnectString() {
		if (this.embedded) {
			return this.zookeeper.getConnectString();
		}
		else {
			return DEFAULT_ZOOKEEPER_CONNECT;
		}
	}

	public ZkClient getZkClient() {
		return this.zkClient;
	}

	public String getBrokerAddress() {
		if (this.embedded) {
			return this.kafkaServer.config().hostName() + ":" + this.kafkaServer.config().port();
		}
		else {
			return DEFAULT_KAFKA_CONNECT;
		}
	}

	@Override
	protected void obtainResource() throws Exception {
		if (!hasFailedAlready) {
			if (this.embedded) {
				try {
					log.debug("Starting Zookeeper");
					this.zookeeper = new EmbeddedZookeeper("127.0.0.1:" + SocketUtils.findAvailableTcpPort());
					log.debug("Started Zookeeper at " + this.zookeeper.getConnectString());
					try {
						int zkConnectionTimeout = 10000;
						int zkSessionTimeout = 10000;
						this.zkClient = new ZkClient(getZkConnectString(), zkSessionTimeout, zkConnectionTimeout,
								ZKStringSerializer$.MODULE$);
					}
					catch (Exception e) {
						this.zookeeper.shutdown();
						throw e;
					}
					try {
						log.debug("Creating Kafka server");
						Properties brokerConfigProperties = this.brokerConfig;
						this.brokerConfig.put("zookeeper.connect", this.zookeeper.getConnectString());
						this.brokerConfig.put("auto.create.topics.enable", "false");
						this.brokerConfig.put("delete.topic.enable", "true");
						this.kafkaServer = TestUtils.createServer(new KafkaConfig(brokerConfigProperties),
								SystemTime$.MODULE$);
						log.debug("Created Kafka server at " + this.kafkaServer.config().hostName() + ":"
								+ this.kafkaServer.config().port());
					}
					catch (Exception e) {
						this.zookeeper.shutdown();
						this.zkClient.close();
						throw e;
					}
				}
				catch (Exception e) {
					hasFailedAlready = true;
					throw e;
				}
			}
			else {
				this.zkClient = new ZkClient(DEFAULT_ZOOKEEPER_CONNECT, 10000, 10000, ZKStringSerializer$.MODULE$);
				if (ZkUtils.getAllBrokersInCluster(this.zkClient).size() == 0) {
					hasFailedAlready = true;
					throw new RuntimeException("Kafka server not available");
				}
			}
		}
		else {
			throw new RuntimeException("Kafka server not available");
		}
	}

	@Override
	protected void cleanupResource() throws Exception {
		if (this.embedded) {
			try {
				this.kafkaServer.shutdown();
			}
			catch (Exception e) {
				// ignore errors on shutdown
				log.error(e.getMessage(), e);
			}
			try {
				Utils.rm(this.kafkaServer.config().logDirs());
			}
			catch (Exception e) {
				// ignore errors on shutdown
				log.error(e.getMessage(), e);
			}
		}
		try {
			this.zkClient.close();
		}
		catch (ZkInterruptedException e) {
			// ignore errors on shutdown
			log.error(e.getMessage(), e);
		}
		if (this.embedded) {
			try {
				this.zookeeper.shutdown();
			}
			catch (Exception e) {
				// ignore errors on shutdown
				log.error(e.getMessage(), e);
			}
		}
	}

}
