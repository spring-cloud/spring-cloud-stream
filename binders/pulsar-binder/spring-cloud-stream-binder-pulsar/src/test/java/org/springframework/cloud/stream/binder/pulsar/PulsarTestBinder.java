/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.provisioning.PulsarTopicProvisioner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;

/**
 * Test binder to exercise producer/consumer bindings in
 * {@link PulsarMessageChannelBinder}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
class PulsarTestBinder extends AbstractPulsarTestBinder {

	@SuppressWarnings({ "unchecked" })
	PulsarTestBinder(PulsarTopicProvisioner pulsarTopicProvisioner, PulsarTemplate<?> pulsarTemplate,
			PulsarConsumerFactory<?> pulsarConsumerFactory, PulsarBinderConfigurationProperties binderConfigProps,
			SchemaResolver schemaResolver, PulsarHeaderMapper headerMapper) {

		try {
			var binder = new PulsarMessageChannelBinder(pulsarTopicProvisioner, (PulsarTemplate<Object>) pulsarTemplate,
					pulsarConsumerFactory, binderConfigProps, schemaResolver, headerMapper);
			var context = new AnnotationConfigApplicationContext(Config.class);
			setApplicationContext(context);
			binder.setApplicationContext(context);
			binder.afterPropertiesSet();
			this.setPollableConsumerBinder(binder);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Configuration
	@EnableIntegration
	static class Config {

	}

}
