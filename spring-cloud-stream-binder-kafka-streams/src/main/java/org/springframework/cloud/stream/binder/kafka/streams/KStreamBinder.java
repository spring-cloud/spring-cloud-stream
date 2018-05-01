/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.cloud.stream.binder.Binder} implementation for {@link KStream}.
 * This implemenation extends from the {@link AbstractBinder} directly.
 * <p>
 * Provides both producer and consumer bindings for the bound KStream.
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
class KStreamBinder extends
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KafkaStreamsConsumerProperties>, ExtendedProducerProperties<KafkaStreamsProducerProperties>>
		implements ExtendedPropertiesBinder<KStream<Object, Object>, KafkaStreamsConsumerProperties, KafkaStreamsProducerProperties> {

	private final static Log LOG = LogFactory.getLog(KStreamBinder.class);

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties = new KafkaStreamsExtendedBindingProperties();

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	private final KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate;

	private final KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	KStreamBinder(KafkaStreamsBinderConfigurationProperties binderConfigurationProperties,
				KafkaTopicProvisioner kafkaTopicProvisioner,
				KafkaStreamsMessageConversionDelegate kafkaStreamsMessageConversionDelegate,
				KafkaStreamsBindingInformationCatalogue KafkaStreamsBindingInformationCatalogue,
				KeyValueSerdeResolver keyValueSerdeResolver) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kafkaStreamsMessageConversionDelegate = kafkaStreamsMessageConversionDelegate;
		this.KafkaStreamsBindingInformationCatalogue = KafkaStreamsBindingInformationCatalogue;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindConsumer(String name, String group,
															KStream<Object, Object> inputTarget,
															ExtendedConsumerProperties<KafkaStreamsConsumerProperties> properties) {
		this.KafkaStreamsBindingInformationCatalogue.registerConsumerProperties(inputTarget, properties.getExtension());
		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<>(
				properties.getExtension());
		if (binderConfigurationProperties.getSerdeError() == KafkaStreamsBinderConfigurationProperties.SerdeError.sendToDlq) {
			extendedConsumerProperties.getExtension().setEnableDlq(true);
		}
		if (!StringUtils.hasText(group)) {
			group = binderConfigurationProperties.getApplicationId();
		}

		String[] inputTopics = StringUtils.commaDelimitedListToStringArray(name);
		for (String inputTopic : inputTopics) {
			this.kafkaTopicProvisioner.provisionConsumerDestination(inputTopic, group, extendedConsumerProperties);
		}

		if (extendedConsumerProperties.getExtension().isEnableDlq()) {
			StreamsConfig streamsConfig = this.KafkaStreamsBindingInformationCatalogue.getStreamsConfig(inputTarget);

			KafkaStreamsDlqDispatch kafkaStreamsDlqDispatch = !StringUtils.isEmpty(extendedConsumerProperties.getExtension().getDlqName()) ?
					new KafkaStreamsDlqDispatch(extendedConsumerProperties.getExtension().getDlqName(), binderConfigurationProperties,
							extendedConsumerProperties.getExtension()) : null;
			for (String inputTopic : inputTopics) {
				if (StringUtils.isEmpty(extendedConsumerProperties.getExtension().getDlqName())) {
					String dlqName = "error." + inputTopic + "." + group;
					kafkaStreamsDlqDispatch = new KafkaStreamsDlqDispatch(dlqName, binderConfigurationProperties,
							extendedConsumerProperties.getExtension());
				}
				SendToDlqAndContinue sendToDlqAndContinue = this.getApplicationContext().getBean(SendToDlqAndContinue.class);
				sendToDlqAndContinue.addKStreamDlqDispatch(inputTopic, kafkaStreamsDlqDispatch);

				DeserializationExceptionHandler deserializationExceptionHandler = streamsConfig.defaultDeserializationExceptionHandler();
				if (deserializationExceptionHandler instanceof SendToDlqAndContinue) {
					((SendToDlqAndContinue) deserializationExceptionHandler).addKStreamDlqDispatch(inputTopic, kafkaStreamsDlqDispatch);
				}
			}
		}

		return new DefaultBinding<>(name, group, inputTarget, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindProducer(String name, KStream<Object, Object> outboundBindTarget,
															ExtendedProducerProperties<KafkaStreamsProducerProperties> properties) {
		ExtendedProducerProperties<KafkaProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(
				new KafkaProducerProperties());
		this.kafkaTopicProvisioner.provisionProducerDestination(name, extendedProducerProperties);
		Serde<?> keySerde = this.keyValueSerdeResolver.getOuboundKeySerde(properties.getExtension());
		Serde<?> valueSerde = this.keyValueSerdeResolver.getOutboundValueSerde(properties, properties.getExtension());
		to(properties.isUseNativeEncoding(), name, outboundBindTarget, (Serde<Object>) keySerde, (Serde<Object>) valueSerde);
		return new DefaultBinding<>(name, null, outboundBindTarget, null);
	}

	@SuppressWarnings("unchecked")
	private void to(boolean isNativeEncoding, String name, KStream<Object, Object> outboundBindTarget,
					Serde<Object> keySerde, Serde<Object> valueSerde) {
		if (!isNativeEncoding) {
			LOG.info("Native encoding is disabled for " + name + ". Outbound message conversion done by Spring Cloud Stream.");
			kafkaStreamsMessageConversionDelegate.serializeOnOutbound(outboundBindTarget)
					.to(name, Produced.with(keySerde, valueSerde));
		} else {
			LOG.info("Native encoding is enabled for " + name + ". Outbound serialization done at the broker.");
			outboundBindTarget.to(name, Produced.with(keySerde, valueSerde));
		}
	}

	@Override
	public KafkaStreamsConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kafkaStreamsExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaStreamsProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kafkaStreamsExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	public void setKafkaStreamsExtendedBindingProperties(KafkaStreamsExtendedBindingProperties kafkaStreamsExtendedBindingProperties) {
		this.kafkaStreamsExtendedBindingProperties = kafkaStreamsExtendedBindingProperties;
	}
}
