/*
 * Copyright 2017 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.admin.AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.integration.test.util.TestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 1.2.2
 *
 */
public class KafkaBinderUnitTests {

	@Test
	public void testPropertyOverrides() throws Exception {
		KafkaBinderConfigurationProperties binderConfigurationProperties = new KafkaBinderConfigurationProperties();
		AdminUtilsOperation adminUtilsOperation = mock(AdminUtilsOperation.class);
		KafkaTopicProvisioner provisioningProvider = new KafkaTopicProvisioner(binderConfigurationProperties,
				adminUtilsOperation);
		KafkaMessageChannelBinder binder = new KafkaMessageChannelBinder(binderConfigurationProperties,
				provisioningProvider);
		KafkaConsumerProperties consumerProps = new KafkaConsumerProperties();
		ExtendedConsumerProperties<KafkaConsumerProperties> ecp =
				new ExtendedConsumerProperties<KafkaConsumerProperties>(consumerProps);
		Method method = KafkaMessageChannelBinder.class.getDeclaredMethod("createKafkaConsumerFactory", boolean.class,
				String.class, ExtendedConsumerProperties.class);
		method.setAccessible(true);

		// test default for anon
		Object factory = method.invoke(binder, true, "foo", ecp);
		Map<?, ?> configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("latest");

		// test default for named
		factory = method.invoke(binder, false, "foo", ecp);
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");

		// binder level setting
		binderConfigurationProperties.setConfiguration(
				Collections.<String, Object>singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
		factory = method.invoke(binder, false, "foo", ecp);
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("latest");

		// consumer level setting
		consumerProps.setConfiguration(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		factory = method.invoke(binder, false, "foo", ecp);
		configs = TestUtils.getPropertyValue(factory, "configs", Map.class);
		assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
	}

}
