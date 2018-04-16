/*
 * Copyright 2015-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.config;

import java.io.IOException;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.properties.JaasLoginModuleConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 * @author Henryk Konsek
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Artem Bilan
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({KafkaAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class, KafkaBinderHealthIndicatorConfiguration.class })
@EnableConfigurationProperties({ KafkaExtendedBindingProperties.class })
public class KafkaBinderConfiguration {

	@Autowired
	private KafkaExtendedBindingProperties kafkaExtendedBindingProperties;

	@Autowired
	private ProducerListener producerListener;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Bean
	KafkaBinderConfigurationProperties configurationProperties(KafkaProperties kafkaProperties) {
		return new KafkaBinderConfigurationProperties(kafkaProperties);
	}

	@Bean
	KafkaTopicProvisioner provisioningProvider(KafkaBinderConfigurationProperties configurationProperties) {
		return new KafkaTopicProvisioner(configurationProperties, this.kafkaProperties);
	}

	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder(KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider) {

		KafkaMessageChannelBinder kafkaMessageChannelBinder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider);
		kafkaMessageChannelBinder.setProducerListener(producerListener);
		kafkaMessageChannelBinder.setExtendedBindingProperties(this.kafkaExtendedBindingProperties);
		return kafkaMessageChannelBinder;
	}

	@Bean
	@ConditionalOnMissingBean(ProducerListener.class)
	ProducerListener producerListener() {
		return new LoggingProducerListener();
	}

	@Bean
	public KafkaJaasLoginModuleInitializer jaasInitializer() throws IOException {
		return new KafkaJaasLoginModuleInitializer();
	}

	/**
	 * A conditional configuration for the {@link KafkaBinderMetrics} bean when the
	 * {@link MeterRegistry} class is in classpath, as well as a {@link MeterRegistry} bean is
	 * present in the application context.
	 */
	@Configuration
	@ConditionalOnClass(MeterRegistry.class)
	@ConditionalOnBean(MeterRegistry.class)
	protected class KafkaBinderMetricsConfiguration {

		@Bean
		@ConditionalOnMissingBean(KafkaBinderMetrics.class)
		public MeterBinder kafkaBinderMetrics(KafkaMessageChannelBinder kafkaMessageChannelBinder,
				KafkaBinderConfigurationProperties configurationProperties,
				MeterRegistry meterRegistry) {

			return new KafkaBinderMetrics(kafkaMessageChannelBinder, configurationProperties, null, meterRegistry);
		}

	}

	public static class JaasConfigurationProperties {

		private JaasLoginModuleConfiguration kafka;

		private JaasLoginModuleConfiguration zookeeper;
	}

}
