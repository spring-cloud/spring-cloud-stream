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

package org.springframework.cloud.stream.binder.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.ClassRule;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka09AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.configuration.KafkaBinderConfigurationProperties;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.retry.RetryOperations;

/**
 * Integration tests for the {@link KafkaMessageChannelBinder}.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class Kafka09BinderTests extends KafkaBinderTests {

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 10);

	private Kafka09TestBinder binder;

	private Kafka09AdminUtilsOperation adminUtilsOperation = new Kafka09AdminUtilsOperation();

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Override
	protected Kafka09TestBinder getBinder() {
		if (binder == null) {
			KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
			binder = new Kafka09TestBinder(binderConfiguration);
		}
		return binder;
	}

	protected KafkaBinderConfigurationProperties createConfigurationProperties() {
		KafkaBinderConfigurationProperties binderConfiguration = new KafkaBinderConfigurationProperties();
		BrokerAddress[] brokerAddresses = embeddedKafka.getBrokerAddresses();
		List<String> bAddresses = new ArrayList<>();
		for (BrokerAddress bAddress : brokerAddresses) {
			bAddresses.add(bAddress.toString());
		}
		String[] foo = new String[bAddresses.size()];
		binderConfiguration.setBrokers(bAddresses.toArray(foo));
		binderConfiguration.setZkNodes(embeddedKafka.getZookeeperConnectionString());
		return binderConfiguration;
	}

	@Override
	protected int partitionSize(String topic) {
		return consumerFactory().createConsumer().partitionsFor(topic).size();
	}

	@Override
	protected void setMetadataRetryOperations(Binder binder, RetryOperations retryOperations) {
		((Kafka09TestBinder) binder).getBinder().setMetadataRetryOperations(retryOperations);
	}

	@Override
	protected ZkUtils getZkUtils(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties) {
		final ZkClient zkClient = new ZkClient(kafkaBinderConfigurationProperties.getZkConnectionString(),
				kafkaBinderConfigurationProperties.getZkSessionTimeout(), kafkaBinderConfigurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);

		return new ZkUtils(zkClient, null, false);
	}

	@Override
	protected void invokeCreateTopic(ZkUtils zkUtils, String topic, int partitions, int replicationFactor, Properties topicConfig) {
		adminUtilsOperation.invokeCreateTopic(zkUtils, topic, partitions, replicationFactor, new Properties());
	}

	@Override
	protected int invokePartitionSize(String topic, ZkUtils zkUtils) {
		return adminUtilsOperation.partitionSize(topic, zkUtils);
	}

	@Override
	protected ExtendedConsumerProperties<KafkaConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new KafkaConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<KafkaProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new KafkaProducerProperties());
	}

	@Override
	public String getKafkaOffsetHeaderKey() {
		return KafkaHeaders.OFFSET;
	}

	@Override
	protected Binder getBinder(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties) {
		return new Kafka09TestBinder(kafkaBinderConfigurationProperties);
	}

	@Before
	public void init() {
		String multiplier = System.getenv("KAFKA_TIMEOUT_MULTIPLIER");
		if (multiplier != null) {
			timeoutMultiplier = Double.parseDouble(multiplier);
		}
	}

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	public Spy spyOn(final String name) {
		throw new UnsupportedOperationException("'spyOn' is not used by Kafka tests");
	}

	private ConsumerFactory<byte[], byte[]> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getKafkaConnectionString());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		Deserializer<byte[]> valueDecoder = new ByteArrayDeserializer();
		Deserializer<byte[]> keyDecoder = new ByteArrayDeserializer();

		return new DefaultKafkaConsumerFactory<>(props, keyDecoder, valueDecoder);
	}

}
