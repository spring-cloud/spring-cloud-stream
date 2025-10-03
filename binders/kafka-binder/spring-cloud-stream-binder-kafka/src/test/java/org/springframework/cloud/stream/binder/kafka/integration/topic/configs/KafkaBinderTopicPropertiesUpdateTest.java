/*
 * Copyright 2018-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.kafka.integration.topic.configs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Heiko Does
 */
@TestPropertySource(properties = "spring.cloud.stream.kafka.binder.autoAlterTopics=true")
class KafkaBinderTopicPropertiesUpdateTest extends BaseKafkaBinderTopicPropertiesUpdateTest {

	@Test
	void kafkaBinderUpdateTopicConfiguration() throws Exception {
		Map<String, Object> adminClientConfig = new HashMap<>();
		adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
		AdminClient adminClient = AdminClient.create(adminClientConfig);
		ConfigResource standardInConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, "standard-in");
		ConfigResource standardOutConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, "standard-out");
		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays
				.asList(standardInConfigResource, standardOutConfigResource));
		KafkaFuture<Map<ConfigResource, Config>> kafkaFuture = describeConfigsResult.all();
		Map<ConfigResource, Config> configResourceConfigMap = kafkaFuture.get(3, TimeUnit.SECONDS);
		Config standardInTopicConfig = configResourceConfigMap.get(standardInConfigResource);
		assertThat(standardInTopicConfig.get("retention.ms").value()).isEqualTo("9001");

		Config standardOutTopicConfig = configResourceConfigMap.get(standardOutConfigResource);
		assertThat(standardOutTopicConfig.get("retention.ms").value()).isEqualTo("9001");
	}
}
