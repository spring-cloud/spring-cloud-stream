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

package org.springframework.cloud.stream.binder.kafka.config;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.AppInfoParser;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderHealthIndicator;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderJaasInitializerListener;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.admin.AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka09AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.admin.Kafka10AdminUtilsOperation;
import org.springframework.cloud.stream.binder.kafka.properties.JaasLoginModuleConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.config.codec.kryo.KryoCodecAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.integration.codec.Codec;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.util.ObjectUtils;

/**
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({KryoCodecAutoConfiguration.class, PropertyPlaceholderAutoConfiguration.class, KafkaBinderConfiguration.KafkaPropertiesConfiguration.class})
@EnableConfigurationProperties({KafkaBinderConfigurationProperties.class, KafkaExtendedBindingProperties.class})
public class KafkaBinderConfiguration {

	protected static final Log logger = LogFactory.getLog(KafkaBinderConfiguration.class);

	@Autowired
	private Codec codec;

	@Autowired
	private KafkaBinderConfigurationProperties configurationProperties;

	@Autowired
	private KafkaExtendedBindingProperties kafkaExtendedBindingProperties;

	@Autowired
	private ProducerListener producerListener;

	@Autowired
	private ApplicationContext context;

	@Autowired (required = false)
	private AdminUtilsOperation adminUtilsOperation;

	@Bean
	KafkaTopicProvisioner provisioningProvider() {
		return new KafkaTopicProvisioner(this.configurationProperties, this.adminUtilsOperation);
	}

	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder() {
		KafkaMessageChannelBinder kafkaMessageChannelBinder = new KafkaMessageChannelBinder(
				this.configurationProperties, provisioningProvider());
		kafkaMessageChannelBinder.setCodec(this.codec);
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
	KafkaBinderHealthIndicator healthIndicator(KafkaMessageChannelBinder kafkaMessageChannelBinder) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		if (!ObjectUtils.isEmpty(configurationProperties.getConfiguration())) {
			props.putAll(configurationProperties.getConfiguration());
		}
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configurationProperties.getKafkaConnectionString());
		ConsumerFactory<?, ?> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
		return new KafkaBinderHealthIndicator(kafkaMessageChannelBinder, consumerFactory);
	}

	@Bean(name = "adminUtilsOperation")
	@Conditional(Kafka09Present.class)
	@ConditionalOnClass(name = "kafka.admin.AdminUtils")
	public AdminUtilsOperation kafka09AdminUtilsOperation() {
		logger.info("AdminUtils selected: Kafka 0.9 AdminUtils");
		return new Kafka09AdminUtilsOperation();
	}

	@Bean(name = "adminUtilsOperation")
	@Conditional(Kafka10Present.class)
	@ConditionalOnClass(name = "kafka.admin.AdminUtils")
	public AdminUtilsOperation kafka10AdminUtilsOperation() {
		logger.info("AdminUtils selected: Kafka 0.10 AdminUtils");
		return new Kafka10AdminUtilsOperation();
	}

	@Bean
	public ApplicationListener<?> jaasInitializer() throws IOException {
		return new KafkaBinderJaasInitializerListener();
	}

	static class Kafka10Present implements Condition {

		@Override
		public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
			return AppInfoParser.getVersion().startsWith("0.10");
		}
	}
	
	static class Kafka09Present implements Condition {

		@Override
		public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
			return AppInfoParser.getVersion().startsWith("0.9");
		}
	}

	public static class JaasConfigurationProperties {

		private JaasLoginModuleConfiguration kafka;

		private JaasLoginModuleConfiguration zookeeper;
	}

	@ConditionalOnClass(name = "org.springframework.boot.autoconfigure.kafka.KafkaProperties")
	public static class KafkaPropertiesConfiguration {

		// KafkaProperties can still be unavailable if KafkaAutoConfiguration is disabled.
		@Autowired(required = false)
		private KafkaProperties kafkaProperties;

		@Autowired
		private KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

		@PostConstruct
		public void init() {
			Map<String, Object> configuration = this.kafkaBinderConfigurationProperties.getConfiguration();
			if (this.kafkaProperties != null) {
				for (Map.Entry<String, String> properties : this.kafkaProperties.getProperties().entrySet()) {
					if (!configuration.containsKey(properties.getKey())) {
						configuration.put(properties.getKey(), properties.getValue());
					}
				}
				for (Map.Entry<String, Object> producerProperties : this.kafkaProperties.buildProducerProperties().entrySet()) {
					if (!configuration.containsKey(producerProperties.getKey())) {
						configuration.put(producerProperties.getKey(), producerProperties.getValue());
					}
				}
				for (Map.Entry<String, Object> consumerProperties : this.kafkaProperties.buildConsumerProperties().entrySet()) {
					if (!configuration.containsKey(consumerProperties.getKey())) {
						configuration.put(consumerProperties.getKey(), consumerProperties.getValue());
					}
				}
				if (ObjectUtils.isEmpty(configuration.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
					configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBinderConfigurationProperties.getKafkaConnectionString());
				}
				else {
					@SuppressWarnings("unchecked")
					List<String> bootStrapServers = (List<String>) configuration.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
					if (bootStrapServers.size() == 1 && bootStrapServers.get(0).equals("localhost:9092")) {
						configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBinderConfigurationProperties.getKafkaConnectionString());
					}
				}
			}
		}
	}
}
