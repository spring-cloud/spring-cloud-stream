/*
 * Copyright 2017-2020 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

	KStreamBinder(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
			KafkaTopicProvisioner kafkaTopicProvisioner,
			KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
			KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
			KeyValueSerdeResolver keyValueSerdeResolver) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.kafkaStreamsBindingInformationCatalogue = KafkaStreamsBindingInformationCatalogue;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
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

		KafkaStreamsBinderUtils.prepareConsumerBinding(name, group,
				getApplicationContext(), this.kafkaTopicProvisioner,
				this.binderConfigurationProperties, properties, retryTemplate, getBeanFactory(), this.kafkaStreamsBindingInformationCatalogue.bindingNamePerTarget(inputTarget));

		return new DefaultBinding<>(name, group, inputTarget, null);
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
				.getOuboundKeySerde(properties.getExtension(), kafkaStreamsBindingInformationCatalogue.getOutboundKStreamResolvable());
		LOG.info("Key Serde used for (outbound) " + name + ": " + keySerde.getClass().getName());

		Serde<?> valueSerde;
		if (properties.isUseNativeEncoding()) {
			valueSerde = this.keyValueSerdeResolver.getOutboundValueSerde(properties,
					properties.getExtension(), kafkaStreamsBindingInformationCatalogue.getOutboundKStreamResolvable());
		}
		else {
			valueSerde = Serdes.ByteArray();
		}
		LOG.info("Value Serde used for (outbound) " + name + ": " + valueSerde.getClass().getName());

		to(properties.isUseNativeEncoding(), name, outboundBindTarget,
				(Serde<Object>) keySerde, (Serde<Object>) valueSerde, properties.getExtension());
		return new DefaultBinding<>(name, null, outboundBindTarget, null);
	}

	@SuppressWarnings("unchecked")
	private void to(boolean isNativeEncoding, String name,
					KStream<Object, Object> outboundBindTarget, Serde<Object> keySerde,
					Serde<Object> valueSerde, KafkaStreamsProducerProperties properties) {
		final Produced<Object, Object> produced = Produced.with(keySerde, valueSerde);
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
