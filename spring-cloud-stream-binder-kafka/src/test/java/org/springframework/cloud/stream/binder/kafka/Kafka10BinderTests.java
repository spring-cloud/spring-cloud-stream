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
import java.util.UUID;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.assertj.core.api.Assertions;
import org.eclipse.jetty.server.Server;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka10AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfigurationProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RetryOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the {@link KafkaMessageChannelBinder}.
 *
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class Kafka10BinderTests extends KafkaBinderTests {

	private final String CLASS_UNDER_TEST_NAME = KafkaMessageChannelBinder.class.getSimpleName();

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 10);

	private Kafka10TestBinder binder;

	private Kafka10AdminUtilsOperation adminUtilsOperation = new Kafka10AdminUtilsOperation();

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Override
	protected Kafka10TestBinder getBinder() {
		if (binder == null) {
			KafkaBinderConfigurationProperties binderConfiguration = createConfigurationProperties();
			binder = new Kafka10TestBinder(binderConfiguration);
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
	@SuppressWarnings("unchecked")
	protected void setMetadataRetryOperations(Binder binder, RetryOperations retryOperations) {
		((Kafka10TestBinder) binder).getBinder().setMetadataRetryOperations(retryOperations);
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
	public String getKafkaOffsetHeaderKey() {
		return KafkaHeaders.OFFSET;
	}

	@Override
	protected Binder getBinder(KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties) {
		return new Kafka10TestBinder(kafkaBinderConfigurationProperties);
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
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "TEST-CONSUMER-GROUP");
		Deserializer<byte[]> valueDecoder = new ByteArrayDeserializer();
		Deserializer<byte[]> keyDecoder = new ByteArrayDeserializer();

		return new DefaultKafkaConsumerFactory<>(props, keyDecoder, valueDecoder);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCustomAvroSerialization() throws Exception {
		KafkaBinderConfigurationProperties configurationProperties = createConfigurationProperties();
		final ZkClient zkClient = new ZkClient(configurationProperties.getZkConnectionString(),
				configurationProperties.getZkSessionTimeout(), configurationProperties.getZkConnectionTimeout(),
				ZKStringSerializer$.MODULE$);
		final ZkUtils zkUtils = new ZkUtils(zkClient, null, false);
		Map<String, Object> schemaRegistryProps = new HashMap<>();
		schemaRegistryProps.put("kafkastore.connection.url", configurationProperties.getZkConnectionString());
		schemaRegistryProps.put("listeners", "http://0.0.0.0:8082");
		schemaRegistryProps.put("port", "8082");
		schemaRegistryProps.put("kafkastore.topic", "_schemas");
		SchemaRegistryConfig config = new SchemaRegistryConfig(schemaRegistryProps);
		SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(config);
		Server server = app.createServer();
		server.start();
		long endTime = System.currentTimeMillis() + 5000;
		while(true) {
			if (server.isRunning()) {
				break;
			}
			else if (System.currentTimeMillis() > endTime) {
				fail("Kafka Schema Registry Server failed to start");
			}
		}
		User1 firstOutboundFoo = new User1();
		String userName1 = "foo-name" + UUID.randomUUID().toString();
		String favColor1 = "foo-color" + UUID.randomUUID().toString();
		firstOutboundFoo.setName(userName1);
		firstOutboundFoo.setFavoriteColor(favColor1);
		Message<?> message = MessageBuilder.withPayload(firstOutboundFoo).build();
		SubscribableChannel moduleOutputChannel = new DirectChannel();
		String testTopicName = "existing" + System.currentTimeMillis();
		invokeCreateTopic(zkUtils, testTopicName, 6, 1, new Properties());
		configurationProperties.setAutoAddPartitions(true);
		Binder binder = getBinder(configurationProperties);
		QueueChannel moduleInputChannel = new QueueChannel();
		ExtendedProducerProperties<KafkaProducerProperties> producerProperties = createProducerProperties();
		producerProperties.getExtension().getConfiguration().put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		producerProperties.getExtension().getConfiguration().put("schema.registry.url", "http://localhost:8082");
		producerProperties.setUseNativeEncoding(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(testTopicName, moduleOutputChannel, producerProperties);
		ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoRebalanceEnabled(false);
		consumerProperties.getExtension().getConfiguration().put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		consumerProperties.getExtension().getConfiguration().put("schema.registry.url", "http://localhost:8082");
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(testTopicName, "test", moduleInputChannel, consumerProperties);
		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		Message<?> inbound = receive(moduleInputChannel);
		assertThat(inbound).isNotNull();
		assertTrue(message.getPayload() instanceof User1);
		User1 receivedUser = (User1) message.getPayload();
		Assertions.assertThat(receivedUser.getName()).isEqualTo(userName1);
		Assertions.assertThat(receivedUser.getFavoriteColor()).isEqualTo(favColor1);
		producerBinding.unbind();
		consumerBinding.unbind();
	}
}
