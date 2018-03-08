/*
 * Copyright 2018 the original author or authors.
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

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaAdminProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BinderFactoryConfiguration;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {KafkaBinderConfiguration.class,
		BinderFactoryConfiguration.class,
		BindingServiceConfiguration.class })
@TestPropertySource(properties = {
		"spring.cloud.stream.kafka.bindings.input.consumer.admin.replication-factor=2",
		"spring.cloud.stream.kafka.bindings.input.consumer.admin.replicas-assignments.0=0,1",
		"spring.cloud.stream.kafka.bindings.input.consumer.admin.configuration.message.format.version=0.9.0.0" })
@EnableIntegration
public class AdminConfigTests {

	@Autowired
	private KafkaMessageChannelBinder binder;

	@Test
	public void testProps() {
		KafkaConsumerProperties consumerProps = this.binder.getExtendedConsumerProperties("input");
		KafkaAdminProperties admin = consumerProps.getAdmin();
		assertThat(admin.getReplicationFactor()).isEqualTo((short) 2);
		assertThat(admin.getReplicasAssignments().get(0)).isEqualTo(Arrays.asList(0, 1));
		assertThat(admin.getConfiguration().get("message.format.version")).isEqualTo("0.9.0.0");
	}
}
