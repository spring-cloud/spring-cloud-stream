/*
 * Copyright 2017-2021 the original author or authors.
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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;

import org.springframework.aop.framework.Advised;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.cloud.stream.binder.Binder} implementation for
 * {@link KStream}. This implemenation extends from the {@link AbstractBinder} directly.
 * <p>
 * Provides both producer and consumer bindings for the bound KStream.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
class KStreamBinder extends
		// @checkstyle:off
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KafkaStreamsConsumerProperties>, ExtendedProducerProperties<KafkaStreamsProducerProperties>>
		implements
		ExtendedPropertiesBinder<KStream<Object, Object>, KafkaStreamsConsumerProperties, KafkaStreamsProducerProperties> {

	// @checkstyle:on

	private static final Log LOG = LogFactory.getLog(KStreamBinder.class);

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	// @checkstyle:off
	private KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties = new KafkaStreamsExtendedBindingProperties();

	// @checkstyle:on

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	private final KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate;

	private final KafkaStreamsBindingInformationCatalogue kafkaStreamsBindingInformationCatalogue;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	private final KafkaStreamsRegistry kafkaStreamsRegistry;

	KStreamBinder(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
				KafkaTopicProvisioner kafkaTopicProvisioner,
				KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
				KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
				KeyValueSerdeResolver keyValueSerdeResolver, KafkaStreamsRegistry kafkaStreamsRegistry) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.kafkaStreamsBindingInformationCatalogue = KafkaStreamsBindingInformationCatalogue;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.kafkaStreamsRegistry = kafkaStreamsRegistry;
	}

	@Override
	protected Binding<KStream<Object, Object>> doBindConsumer(String name, String group,
			KStream<Object, Object> inputTarget,
			ExtendedConsumerProperties<KafkaStreamsConsumerProperties> properties) {

		KStream<Object, Object> delegate = ((KStreamBoundElementFactory.KStreamWrapperHandler)
				((Advised) inputTarget).getAdvisors()[0].getAdvice()).getDelegate();

		this.kafkaStreamsBindingInformationCatalogue.registerConsumerProperties(delegate, properties.getExtension());

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


		return new DefaultBinding<KStream<Object, Object>>(bindingName, group,
				inputTarget, streamsBuilderFactoryBean) {

			@Override
			public boolean isInput() {
				return true;
			}

			@Override
			public synchronized void start() {
				if (!streamsBuilderFactoryBean.isRunning()) {
					super.start();
					KStreamBinder.this.kafkaStreamsRegistry.registerKafkaStreams(streamsBuilderFactoryBean);
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
					KStreamBinder.this.kafkaStreamsRegistry.unregisterKafkaStreams(kafkaStreams);
					KafkaStreamsBinderUtils.closeDlqProducerFactories(kafkaStreamsBindingInformationCatalogue, streamsBuilderFactoryBean);
					//Caching the stopped KafkaStreams for health indicator purposes on the underlying processor.
					//See this issue for more details: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/1165
					KStreamBinder.this.kafkaStreamsBindingInformationCatalogue.addPreviousKafkaStreamsForApplicationId(
							(String) streamsBuilderFactoryBean.getStreamsConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG), kafkaStreams);
				}
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindProducer(String name,
			KStream<Object, Object> outboundBindTarget,
			ExtendedProducerProperties<KafkaStreamsProducerProperties> properties) {

		ExtendedProducerProperties<KafkaProducerProperties> extendedProducerProperties =
				(ExtendedProducerProperties) properties;

		this.kafkaTopicProvisioner.provisionProducerDestination(name, extendedProducerProperties);
		Serde<?> keySerde = this.keyValueSerdeResolver
				.getOuboundKeySerde(properties.getExtension(), kafkaStreamsBindingInformationCatalogue.getOutboundKStreamResolvable(outboundBindTarget));
		LOG.info("Key Serde used for (outbound) " + name + ": " + keySerde.getClass().getName());

		Serde<?> valueSerde;
		if (properties.isUseNativeEncoding()) {
			valueSerde = this.keyValueSerdeResolver.getOutboundValueSerde(properties,
					properties.getExtension(), kafkaStreamsBindingInformationCatalogue.getOutboundKStreamResolvable(outboundBindTarget));
		}
		else {
			valueSerde = Serdes.ByteArray();
		}
		LOG.info("Value Serde used for (outbound) " + name + ": " + valueSerde.getClass().getName());

		to(properties.isUseNativeEncoding(), name, outboundBindTarget,
				(Serde<Object>) keySerde, (Serde<Object>) valueSerde, properties.getExtension());

		final String bindingName = this.kafkaStreamsBindingInformationCatalogue.bindingNamePerTarget(outboundBindTarget);
		final StreamsBuilderFactoryBean streamsBuilderFactoryBean = this.kafkaStreamsBindingInformationCatalogue
				.getStreamsBuilderFactoryBeanPerBinding().get(bindingName);

		// We need the application id to pass to DefaultBinding so that it won't be interpreted as an anonymous group.
		// In case, if we can't find application.id (which is unlikely), we just default to bindingName.
		// This will only be used for lifecycle management through actuator endpoints.
		final Properties streamsConfiguration = streamsBuilderFactoryBean.getStreamsConfiguration();
		final String applicationId = streamsConfiguration != null ? (String) streamsConfiguration.get("application.id") : bindingName;

		return new DefaultBinding<KStream<Object, Object>>(bindingName,
				applicationId, outboundBindTarget, streamsBuilderFactoryBean) {

			@Override
			public boolean isInput() {
				return false;
			}

			@Override
			public synchronized void start() {
				if (!streamsBuilderFactoryBean.isRunning()) {
					super.start();
					KStreamBinder.this.kafkaStreamsRegistry.registerKafkaStreams(streamsBuilderFactoryBean);
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
					KStreamBinder.this.kafkaStreamsRegistry.unregisterKafkaStreams(kafkaStreams);
					KafkaStreamsBinderUtils.closeDlqProducerFactories(kafkaStreamsBindingInformationCatalogue, streamsBuilderFactoryBean);
					//Caching the stopped KafkaStreams for health indicator purposes on the underlying processor
					//See this issue for more details: https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/1165
					KStreamBinder.this.kafkaStreamsBindingInformationCatalogue.addPreviousKafkaStreamsForApplicationId(
							(String) streamsBuilderFactoryBean.getStreamsConfiguration().get(StreamsConfig.APPLICATION_ID_CONFIG), kafkaStreams);
				}
			}
		};
	}

	@SuppressWarnings("unchecked")
	private void to(boolean isNativeEncoding, String name,
					KStream<Object, Object> outboundBindTarget, Serde<Object> keySerde,
					Serde<Object> valueSerde, KafkaStreamsProducerProperties properties) {
		final Produced<Object, Object> produced = Produced.with(keySerde, valueSerde);
		if (StringUtils.hasText(properties.getProducedAs())) {
			produced.withName(properties.getProducedAs());
		}
		StreamPartitioner streamPartitioner = null;
		if (!StringUtils.isEmpty(properties.getStreamPartitionerBeanName())) {
			streamPartitioner = getApplicationContext().getBean(properties.getStreamPartitionerBeanName(),
					StreamPartitioner.class);
		}
		if (streamPartitioner != null) {
			produced.withStreamPartitioner(streamPartitioner);
		}
		if (!isNativeEncoding) {
			LOG.info("Native encoding is disabled for " + name
					+ ". Outbound message conversion done by Spring Cloud Stream.");
			outboundBindTarget.filter((k, v) -> v == null)
					.to(name, produced);
			this.kafkaStreamsMessageConversionDelegate
					.serializeOnOutbound(outboundBindTarget)
					.to(name, produced);
		}
		else {
			LOG.info("Native encoding is enabled for " + name
					+ ". Outbound serialization done at the broker.");
			outboundBindTarget.to(name, produced);
		}
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
		return this.kafkaStreamsExtendedBindingProperties
				.getExtendedProducerProperties(channelName);
	}

	public void setKafkaStreamsExtendedBindingProperties(
			KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties) {
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
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

}
