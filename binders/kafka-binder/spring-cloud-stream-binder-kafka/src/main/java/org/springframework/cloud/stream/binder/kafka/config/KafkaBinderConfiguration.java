/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.config;

import java.io.IOException;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.kafka.KafkaBinderMetrics;
import org.springframework.cloud.stream.binder.kafka.KafkaBindingRebalanceListener;
import org.springframework.cloud.stream.binder.kafka.KafkaMessageChannelBinder;
import org.springframework.cloud.stream.binder.kafka.KafkaNullConverter;
import org.springframework.cloud.stream.binder.kafka.properties.JaasLoginModuleConfiguration;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.AdminClientConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.support.ConsumerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.support.ProducerConfigCustomizer;
import org.springframework.cloud.stream.binder.kafka.utils.DlqDestinationResolver;
import org.springframework.cloud.stream.binder.kafka.utils.DlqPartitionFunction;
import org.springframework.cloud.stream.config.ConsumerEndpointCustomizer;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.config.MessageSourceCustomizer;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConverter;

/**
 * Kafka binder configuration class.
 *
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Soby Chacko
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 * @author Henryk Konsek
 * @author Gary Russell
 * @author Oleg Zhurakousky
 * @author Artem Bilan
 * @author Aldo Sinanaj
 * @author Chris Bono
 * @author Yi Liu
 * @author Byungjun You
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(Binder.class)
@Import({ KafkaBinderHealthIndicatorConfiguration.class })
@EnableConfigurationProperties({ KafkaProperties.class, KafkaExtendedBindingProperties.class })
public class KafkaBinderConfiguration {

	/**
	 * @ConfigurationProperties is declared on the @Bean method for Spring Boot to ignore
	 * constructor binding on KafkaBinderConfigurationProperties. If constructor binding is
	 * used, it ignores all the JavaBeans style properties when generating configuration metadata.
	 *
	 * See the following issues for more details:
	 *
	 * https://github.com/spring-cloud/spring-cloud-stream/issues/2640
	 * https://github.com/spring-projects/spring-boot/issues/34031
	 *
	 * @param kafkaProperties Spring Kafka properties autoconfigured by Spring Boot
	 */
	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kafka.binder")
	KafkaBinderConfigurationProperties configurationProperties(
			KafkaProperties kafkaProperties, ObjectProvider<KafkaConnectionDetails> kafkaConnectionDetails) {
		return new KafkaBinderConfigurationProperties(kafkaProperties, kafkaConnectionDetails);
	}

	@Bean
	KafkaTopicProvisioner provisioningProvider(
			KafkaBinderConfigurationProperties configurationProperties,
			ObjectProvider<AdminClientConfigCustomizer> adminClientConfigCustomizers, KafkaProperties kafkaProperties,
			ObjectProvider<KafkaConnectionDetails> kafkaConnectionDetails) {
		return new KafkaTopicProvisioner(configurationProperties,
				kafkaProperties, kafkaConnectionDetails.getIfAvailable(), adminClientConfigCustomizers.orderedStream().collect(Collectors.toList()));
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Bean
	KafkaMessageChannelBinder kafkaMessageChannelBinder(
			KafkaBinderConfigurationProperties configurationProperties,
			KafkaTopicProvisioner provisioningProvider,
			@Nullable ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> listenerContainerCustomizer,
			@Nullable MessageSourceCustomizer<KafkaMessageSource<?, ?>> sourceCustomizer,
			@Nullable ProducerMessageHandlerCustomizer<KafkaProducerMessageHandler<?, ?>> messageHandlerCustomizer,
			@Nullable ConsumerEndpointCustomizer<KafkaMessageDrivenChannelAdapter<?, ?>> consumerCustomizer,
			ObjectProvider<KafkaBindingRebalanceListener> rebalanceListener,
			ObjectProvider<DlqPartitionFunction> dlqPartitionFunction,
			ObjectProvider<DlqDestinationResolver> dlqDestinationResolver,
			ObjectProvider<ClientFactoryCustomizer> clientFactoryCustomizers,
			ObjectProvider<ConsumerConfigCustomizer> consumerConfigCustomizers,
			ObjectProvider<ProducerConfigCustomizer> producerConfigCustomizers,
			ProducerListener producerListener, KafkaExtendedBindingProperties kafkaExtendedBindingProperties
			) {

		KafkaMessageChannelBinder kafkaMessageChannelBinder = new KafkaMessageChannelBinder(
				configurationProperties, provisioningProvider,
				listenerContainerCustomizer, sourceCustomizer, rebalanceListener.getIfUnique(),
				dlqPartitionFunction.getIfUnique(), dlqDestinationResolver.getIfUnique());
		kafkaMessageChannelBinder.setProducerListener(producerListener);
		kafkaMessageChannelBinder
				.setExtendedBindingProperties(kafkaExtendedBindingProperties);
		kafkaMessageChannelBinder.setProducerMessageHandlerCustomizer(messageHandlerCustomizer);
		kafkaMessageChannelBinder.setConsumerEndpointCustomizer(consumerCustomizer);
		clientFactoryCustomizers.orderedStream().forEach(kafkaMessageChannelBinder::addClientFactoryCustomizer);
		consumerConfigCustomizers.orderedStream().forEach(kafkaMessageChannelBinder::addConsumerConfigCustomizer);
		producerConfigCustomizers.orderedStream().forEach(kafkaMessageChannelBinder::addProducerConfigCustomizer);
		return kafkaMessageChannelBinder;
	}

	@SuppressWarnings("rawtypes")
	@Bean
	@ConditionalOnMissingBean(ProducerListener.class)
	ProducerListener producerListener() {
		return new LoggingProducerListener();
	}

	@Bean
	@ConditionalOnMissingBean(KafkaNullConverter.class)
	MessageConverter kafkaNullConverter() {
		return new KafkaNullConverter();
	}

	@Bean
	@ConditionalOnMissingBean(KafkaJaasLoginModuleInitializer.class)
	public KafkaJaasLoginModuleInitializer jaasInitializer(
			KafkaBinderConfigurationProperties configurationProperties)
			throws IOException {
		JaasLoginModuleConfiguration jaas = configurationProperties.getJaas();
		if (jaas == null) {
			return null;
		}
		else {
			KafkaJaasLoginModuleInitializer kafkaJaasLoginModuleInitializer = new KafkaJaasLoginModuleInitializer();
			kafkaJaasLoginModuleInitializer.setLoginModule(jaas.getLoginModule());

			KafkaJaasLoginModuleInitializer.ControlFlag controlFlag = jaas
				.getControlFlag();

			if (controlFlag != null) {
				kafkaJaasLoginModuleInitializer.setControlFlag(controlFlag);
			}
			kafkaJaasLoginModuleInitializer.setOptions(jaas.getOptions());
			return kafkaJaasLoginModuleInitializer;
		}
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnMissingBean(value = KafkaBinderMetrics.class, name = "outerContext")
	@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
	protected class KafkaBinderMetricsConfiguration {

		@Bean
		@ConditionalOnBean(MeterRegistry.class)
		@ConditionalOnMissingBean(KafkaBinderMetrics.class)
		public MeterBinder kafkaBinderMetrics(
				KafkaMessageChannelBinder kafkaMessageChannelBinder,
				KafkaBinderConfigurationProperties configurationProperties,
				MeterRegistry meterRegistry) {

			return new KafkaBinderMetrics(kafkaMessageChannelBinder,
					configurationProperties, null, meterRegistry);
		}

		@ConditionalOnClass(name = "org.springframework.kafka.core.MicrometerConsumerListener")
		@ConditionalOnBean(MeterRegistry.class)
		protected class KafkaMicrometer {

			@Bean
			@ConditionalOnMissingBean(name = "binderClientFactoryCustomizer")
			public ClientFactoryCustomizer binderClientFactoryCustomizer(MeterRegistry meterRegistry) {

				return new ClientFactoryCustomizer() {

					@Override
					public void configure(ProducerFactory<?, ?> pf) {
						if (pf instanceof DefaultKafkaProducerFactory) {
							pf.addListener(new MicrometerProducerListener<>(meterRegistry));
						}
					}

					@Override
					public void configure(ConsumerFactory<?, ?> cf) {
						if (cf instanceof DefaultKafkaConsumerFactory) {
							cf.addListener(new MicrometerConsumerListener<>(meterRegistry));
						}
					}

				};

			}

		}

	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnBean(name = "outerContext")
	@ConditionalOnMissingBean(KafkaBinderMetrics.class)
	@ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
	protected class KafkaBinderMetricsConfigurationWithMultiBinder {

		@Bean
		public MeterBinder kafkaBinderMetrics(
				KafkaMessageChannelBinder kafkaMessageChannelBinder,
				KafkaBinderConfigurationProperties configurationProperties,
				ConfigurableApplicationContext context) {

			MeterRegistry meterRegistry = context.getBean("outerContext", ApplicationContext.class)
					.getBean(MeterRegistry.class);
			return new KafkaBinderMetrics(kafkaMessageChannelBinder,
					configurationProperties, null, meterRegistry);
		}

		@ConditionalOnClass(name = "org.springframework.kafka.core.MicrometerConsumerListener")
		protected class KafkaMicrometer {

			@Bean
			@ConditionalOnMissingBean(name = "binderClientFactoryCustomizer")
			public ClientFactoryCustomizer binderClientFactoryCustomizer(ConfigurableApplicationContext context) {


				return new ClientFactoryCustomizer() {

					MeterRegistry meterRegistry = context.getBean("outerContext", ApplicationContext.class)
							.getBean(MeterRegistry.class);

					@Override
					public void configure(ProducerFactory<?, ?> pf) {
						if (pf instanceof DefaultKafkaProducerFactory) {
							pf.addListener(new MicrometerProducerListener<>(this.meterRegistry));
						}
					}

					@Override
					public void configure(ConsumerFactory<?, ?> cf) {
						if (cf instanceof DefaultKafkaConsumerFactory) {
							cf.addListener(new MicrometerConsumerListener<>(this.meterRegistry));
						}
					}

				};

			}

		}

	}

	/**
	 * Properties configuration for Jaas.
	 */
	@SuppressWarnings("unused")
	public static class JaasConfigurationProperties {

		private JaasLoginModuleConfiguration kafka;

		private JaasLoginModuleConfiguration zookeeper;

	}
}
