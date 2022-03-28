/*
 * Copyright 2019-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import java.io.IOException;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.kafka.properties.JaasLoginModuleConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;

/**
 * Jaas configuration bean for Kafka Streams binder types.
 *
 * @author Soby Chacko
 * @since 3.1.4
 */
@Configuration
public class KafkaStreamsJaasConfiguration {

	@Bean
	@ConditionalOnMissingBean(KafkaJaasLoginModuleInitializer.class)
	public KafkaJaasLoginModuleInitializer jaasInitializer(
			KafkaBinderConfigurationProperties configurationProperties)
			throws IOException {
		KafkaJaasLoginModuleInitializer kafkaJaasLoginModuleInitializer = new KafkaJaasLoginModuleInitializer();
		JaasLoginModuleConfiguration jaas = configurationProperties.getJaas();
		if (jaas != null) {
			kafkaJaasLoginModuleInitializer.setLoginModule(jaas.getLoginModule());

			KafkaJaasLoginModuleInitializer.ControlFlag controlFlag = jaas
					.getControlFlag();

			if (controlFlag != null) {
				kafkaJaasLoginModuleInitializer.setControlFlag(controlFlag);
			}
			kafkaJaasLoginModuleInitializer.setOptions(jaas.getOptions());
		}
		return kafkaJaasLoginModuleInitializer;
	}
}
