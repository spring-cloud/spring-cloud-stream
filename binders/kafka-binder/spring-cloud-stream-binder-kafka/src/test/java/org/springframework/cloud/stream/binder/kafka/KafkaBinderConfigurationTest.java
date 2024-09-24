/*
 * Copyright 2016-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.stream.binder.kafka.config.ClientFactoryCustomizer;
import org.springframework.cloud.stream.binder.kafka.config.KafkaBinderConfiguration;
import org.springframework.cloud.stream.binder.kafka.provisioning.AdminClientConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;

import static org.apache.kafka.clients.admin.AdminClientConfig.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.util.ReflectionUtils.getField;

/**
 * @author Ilayaperumal Gopinathan
 * @author Chris Bono
 */
@SpringBootTest(classes = { KafkaBinderConfiguration.class })
class KafkaBinderConfigurationTest {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(KafkaBinderConfiguration.class, KafkaAutoConfiguration.class);

	@Test
	void binderAutoConfiguredWithProducerListener(@Autowired KafkaMessageChannelBinder kafkaMessageChannelBinder) {
		assertThat(ReflectionTestUtils.getField(kafkaMessageChannelBinder, "producerListener")).isNotNull();
	}

	@Test
	public void multipleClientFactoryCustomizers() {
		contextRunner.withUserConfiguration(ClientFactoryCustomizerConfigs.class)
					.run(context -> {
						assertThat(context).hasSingleBean(KafkaMessageChannelBinder.class);
						KafkaMessageChannelBinder kafkaMessageChannelBinder =
							context.getBean(KafkaMessageChannelBinder.class);
						Map<String, ClientFactoryCustomizer> customizers =
							context.getBeansOfType(ClientFactoryCustomizer.class);
						assertThat(customizers).hasSize(2);
						Field clientFactoryCustomizersField = ReflectionUtils.findField(
							KafkaMessageChannelBinder.class, "clientFactoryCustomizers",
							List.class);
						ReflectionUtils.makeAccessible(clientFactoryCustomizersField);
						List<ClientFactoryCustomizer> clientFactoryCustomizers =
							(List<ClientFactoryCustomizer>) getField(clientFactoryCustomizersField, kafkaMessageChannelBinder);
						assertThat(clientFactoryCustomizers).hasSize(2);
					});
	}

	@Test
	public void multipleAdminClientConfigCustomizers() {
		contextRunner.withUserConfiguration(AdminClientConfigCustomizerConfigs.class)
					.run(context -> {
						assertThat(context).hasSingleBean(KafkaTopicProvisioner.class);
						KafkaTopicProvisioner kafkaTopicProvisioner = context.getBean(KafkaTopicProvisioner.class);
						Map<String, AdminClientConfigCustomizer> customizers =
							context.getBeansOfType(AdminClientConfigCustomizer.class);
						assertThat(customizers).hasSize(2);
						Field adminClientPropertiesField = ReflectionUtils.findField(
							KafkaTopicProvisioner.class, "adminClientProperties", Map.class);
						ReflectionUtils.makeAccessible(adminClientPropertiesField);
						Map<String, Object> adminClientProperties = (Map<String, Object>) ReflectionUtils.
							getField(adminClientPropertiesField, kafkaTopicProvisioner);
						assertThat(adminClientProperties.get(SECURITY_PROTOCOL_CONFIG)).isEqualTo("SASL_SSL");
						assertThat(adminClientProperties.get(SASL_MECHANISM)).isEqualTo("OAUTHBEARER");
					});
	}

	@Configuration
	static class ClientFactoryCustomizerConfigs {

		@Bean
		ClientFactoryCustomizer testClientFactoryCustomizerOne() {
			return new ClientFactoryCustomizer() {
				@Override
				public void configure(ProducerFactory<?, ?> pf) {
					ClientFactoryCustomizer.super.configure(pf);
				}
			};
		}

		@Bean
		ClientFactoryCustomizer testClientFactoryCustomizerTwo() {
			return new ClientFactoryCustomizer() {
				@Override
				public void configure(ProducerFactory<?, ?> pf) {
					ClientFactoryCustomizer.super.configure(pf);
				}
			};
		}
	}

	@Configuration
	static class AdminClientConfigCustomizerConfigs {

		@Bean
		AdminClientConfigCustomizer testAdminClientConfigCustomizerOne() {
			return prop -> prop.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		}

		@Bean
		AdminClientConfigCustomizer testAdminClientConfigCustomizerTwo() {
			return prop -> prop.put(SASL_MECHANISM, "OAUTHBEARER");
		}
	}

}
