/*
 * Copyright 2015-2017 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.properties.JaasLoginModuleConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 * @author Henryk Konsek
 * @author Gary Russell
 * @author Oleg Zhurakousky
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({ PropertyPlaceholderAutoConfiguration.class})
@EnableConfigurationProperties({ KafkaExtendedBindingProperties.class })
public class KafkaBinderConfiguration {

	protected static final Log logger = LogFactory.getLog(KafkaBinderConfiguration.class);

	@Autowired
	private KafkaExtendedBindingProperties kafkaExtendedBindingProperties;

	@Autowired
	private ProducerListener producerListener;

	@Autowired
	private ApplicationContext context;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Bean
	KafkaBinderConfigurationProperties configurationProperties() {
		return new KafkaBinderConfigurationProperties();
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
	KafkaBinderHealthIndicator healthIndicator(KafkaMessageChannelBinder kafkaMessageChannelBinder,
											KafkaBinderConfigurationProperties configurationProperties) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		if (!ObjectUtils.isEmpty(configurationProperties.getConsumerConfiguration())) {
			props.putAll(configurationProperties.getConsumerConfiguration());
		}
		if (!props.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.getKafkaConnectionString());
		}
		ConsumerFactory<?, ?> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
		KafkaBinderHealthIndicator indicator = new KafkaBinderHealthIndicator(kafkaMessageChannelBinder,
				consumerFactory);
		indicator.setTimeout(configurationProperties.getHealthTimeout());
		return indicator;
	}

	@Bean
	public MeterBinder kafkaBinderMetrics(KafkaMessageChannelBinder kafkaMessageChannelBinder,
										KafkaBinderConfigurationProperties configurationProperties,
										@Nullable MeterRegistry meterRegistry) {
		return new KafkaBinderMetrics(kafkaMessageChannelBinder, configurationProperties, null, meterRegistry);
	}

	@Bean
	public KafkaJaasLoginModuleInitializer jaasInitializer() throws IOException {
		return new KafkaJaasLoginModuleInitializer();
	}

	public static class JaasConfigurationProperties {

		private JaasLoginModuleConfiguration kafka;

		private JaasLoginModuleConfiguration zookeeper;
	}
}
