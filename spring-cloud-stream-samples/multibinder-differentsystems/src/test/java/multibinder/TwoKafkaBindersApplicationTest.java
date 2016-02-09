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

package multibinder;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import java.util.UUID;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.test.junit.kafka.KafkaTestSupport;
import org.springframework.cloud.stream.test.junit.redis.RedisTestSupport;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.integration.kafka.core.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = MultibinderApplication.class)
@WebAppConfiguration
@DirtiesContext
public class TwoKafkaBindersApplicationTest {

	@ClassRule
	public static KafkaTestSupport kafkaTestSupport1 = new KafkaTestSupport(true);

	@ClassRule
	public static KafkaTestSupport kafkaTestSupport2 = new KafkaTestSupport(true);

	@ClassRule
	public static RedisTestSupport redisTestSupport = new RedisTestSupport();

	@BeforeClass
	public static void setupEnvironment() {
		System.setProperty("kafkaBroker1", kafkaTestSupport1.getBrokerAddress());
		System.setProperty("zk1", kafkaTestSupport1.getZkConnectString());
		System.setProperty("kafkaBroker2", kafkaTestSupport2.getBrokerAddress());
		System.setProperty("zk2", kafkaTestSupport2.getZkConnectString());
	}

	@Autowired
	private BinderFactory<MessageChannel> binderFactory;

	@Test
	public void contextLoads() {
		KafkaMessageChannelBinder kafka1 = (KafkaMessageChannelBinder) binderFactory.getBinder("kafka1");
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(kafka1.getConnectionFactory());
		Configuration configuration = (Configuration) directFieldAccessor.getPropertyValue("configuration");
		List<BrokerAddress> brokerAddresses = configuration.getBrokerAddresses();
		Assert.assertThat(brokerAddresses, hasSize(1));
		Assert.assertThat(brokerAddresses, contains(BrokerAddress.fromAddress(kafkaTestSupport1.getBrokerAddress())));
		KafkaMessageChannelBinder kafka2 = (KafkaMessageChannelBinder) binderFactory.getBinder("kafka2");
		DirectFieldAccessor directFieldAccessor2 = new DirectFieldAccessor(kafka2.getConnectionFactory());
		Configuration configuration2 = (Configuration) directFieldAccessor2.getPropertyValue("configuration");
		List<BrokerAddress> brokerAddresses2 = configuration2.getBrokerAddresses();
		Assert.assertThat(brokerAddresses2, hasSize(1));
		Assert.assertThat(brokerAddresses2, contains(BrokerAddress.fromAddress(kafkaTestSupport2.getBrokerAddress())));
	}

	@Test
	public void messagingWorks() {
		DirectChannel dataProducer = new DirectChannel();
		binderFactory.getBinder("kafka1").bindProducer("dataIn", dataProducer, null);

		QueueChannel dataConsumer = new QueueChannel();
		binderFactory.getBinder("kafka2").bindConsumer("dataOut", UUID.randomUUID().toString(),
				dataConsumer, null);

		String testPayload = "testFoo" + UUID.randomUUID().toString();
		dataProducer.send(MessageBuilder.withPayload(testPayload).build());

		Message<?> receive = dataConsumer.receive(2000);
		Assert.assertThat(receive, Matchers.notNullValue());
		Assert.assertThat(receive.getPayload(), CoreMatchers.equalTo(testPayload));
	}

}
