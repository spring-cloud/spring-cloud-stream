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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.StringUtils;

/**
 * An {@link AbstractBinder} implementation for {@link GlobalKTable}.
 * <p>
 * Provides only consumer binding for the bound {@link GlobalKTable}. Output bindings are
 * not allowed on this binder.
 *
 * @author Soby Chacko
 * @since 2.1.0
 */
public class GlobalKTableBinder extends
		// @checkstyle:off
		AbstractBinder<GlobalKTable<Object, Object>, ExtendedConsumerProperties<KafkaStreamsConsumerProperties>, ExtendedProducerProperties<KafkaStreamsProducerProperties>>
		implements
		ExtendedPropertiesBinder<GlobalKTable<Object, Object>, KafkaStreamsConsumerProperties, KafkaStreamsProducerProperties> {

	// @checkstyle:on
	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	// @checkstyle:off
	private KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties = new KafkaStreamsExtendedBindingProperties();
	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	// @checkstyle:on

	public GlobalKTableBinder(
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
			KafkaTopicProvisioner kafkaTopicProvisioner,
			KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue, KafkaStreamsRegistry kafkaStreamsRegistry) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kafkaStreamsBindingInformationCatalogue = kafkaStreamsBindingInformationCatalogue;
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<GlobalKTable<Object, Object>> doBindConsumer(String name,
			String group, GlobalKTable<Object, Object> inputTarget,
			ExtendedConsumerProperties<KafkaStreamsConsumerProperties> properties) {
		if (!StringUtils.hasText(group)) {
			group = properties.getExtension().getApplicationId();
		}
		final RetryTemplate retryTemplate = buildRetryTemplate(properties);

		final String bindingName = this.kafkaStreamsBindingInformationCatalogue.bindingNamePerTarget(inputTarget);
		final StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.kafkaStreamsBindingInformationCatalogue
				.getStreamsBuilderFactoryBeanPerBinding().get(bindingName);

		KafkaStreamsBinderUtils.prepareConsumerBinding(name, group,
				getApplicationContext(), this.kafkaTopicProvisioner,
				this.binderConfigurationProperties, properties, retryTemplate, getBeanFactory(),
				this.kafkaStreamsBindingInformationCatalogue.bindingNamePerTarget(inputTarget),
				this.kafkaStreamsBindingInformationCatalogue, streamsBuilderFactoryBean);

		return new DefaultBinding<>(bindingName, group, inputTarget, streamsBuilderFactoryBean) {

			@Override
			public boolean isInput() {
				return true;
			}

			@Override
			public synchronized void start() {
				if (!streamsBuilderFactoryBean.isRunning()) {
					super.start();
					GlobalKTableBinder.this.kafkaStreamsRegistry.registerKafkaStreams(streamsBuilderFactoryBean);
					//If we cached the previous KafkaStreams object (from a binding stop on the actuator), remove it.
					//See this issue for more details: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/1165
					final String applicationId = (String) streamsBuilderFactoryBean.getStreamsConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG);
					if (kafkaStreamsBindingInformationCatalogue.getStoppedKafkaStreams().containsKey(applicationId)) {
						kafkaStreamsBindingInformationCatalogue.removePreviousKafkaStreamsForApplicationId(applicationId);
					}
				}
			}

			@Override
			public synchronized void stop() {
				if (streamsBuilderFactoryBean.isRunning()) {
					final KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
					super.stop();
					GlobalKTableBinder.this.kafkaStreamsRegistry.unregisterKafkaStreams(kafkaStreams);
					KafkaStreamsBinderUtils.closeDlqProducerFactories(kafkaStreamsBindingInformationCatalogue, streamsBuilderFactoryBean);
					//Caching the stopped KafkaStreams for health indicator purposes on the underlying processor.
					//See this issue for more details: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/1165
					GlobalKTableBinder.this.kafkaStreamsBindingInformationCatalogue.addPreviousKafkaStreamsForApplicationId(
						(String) streamsBuilderFactoryBean.getStreamsConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG), kafkaStreams);
				}
			}
		};
	}

	@Override
	protected Binding<GlobalKTable<Object, Object>> doBindProducer(String name,
			GlobalKTable<Object, Object> outboundBindTarget,
			ExtendedProducerProperties<KafkaStreamsProducerProperties> properties) {
		throw new UnsupportedOperationException(
				"No producer level binding is allowed for GlobalKTable");
	}

	@Override
	public KafkaStreamsConsumerProperties getExtendedConsumerProperties(
			String channelName) {
		return this.kafkaStreamsExtendedBindingProperties
				.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaStreamsProducerProperties getExtendedProducerProperties(
			String channelName) {
		throw new UnsupportedOperationException(
				"No producer binding is allowed and therefore no properties");
	}

	@Override
	public String getDefaultsPrefix() {
		return this.kafkaStreamsExtendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.kafkaStreamsExtendedBindingProperties
				.getExtendedPropertiesEntryClass();
	}

	public void setKafkaStreamsExtendedBindingProperties(
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties) {
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
	}

}
