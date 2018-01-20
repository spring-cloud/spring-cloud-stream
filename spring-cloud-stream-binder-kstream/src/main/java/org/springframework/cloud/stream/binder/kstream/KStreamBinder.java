/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.binder.kstream;

import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kstream.config.KStreamBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamBinder extends
		AbstractBinder<KStream<Object, Object>, ExtendedConsumerProperties<KStreamConsumerProperties>, ExtendedProducerProperties<KStreamProducerProperties>>
		implements ExtendedPropertiesBinder<KStream<Object, Object>, KStreamConsumerProperties, KStreamProducerProperties> {

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private KStreamExtendedBindingProperties kStreamExtendedBindingProperties = new KStreamExtendedBindingProperties();

	private final KStreamBinderConfigurationProperties binderConfigurationProperties;

	private final KStreamBoundMessageConversionDelegate kStreamBoundMessageConversionDelegate;

	private final KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue;

	private final KeyValueSerdeResolver keyValueSerdeResolver;

	private final QueryableStoreRegistry queryableStoreRegistry;

	public KStreamBinder(KStreamBinderConfigurationProperties binderConfigurationProperties,
						KafkaTopicProvisioner kafkaTopicProvisioner,
						KStreamBoundMessageConversionDelegate kStreamBoundMessageConversionDelegate,
						KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue,
						KeyValueSerdeResolver keyValueSerdeResolver,
						QueryableStoreRegistry queryableStoreRegistry) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kStreamBoundMessageConversionDelegate = kStreamBoundMessageConversionDelegate;
		this.KStreamBindingInformationCatalogue = KStreamBindingInformationCatalogue;
		this.keyValueSerdeResolver = keyValueSerdeResolver;
		this.queryableStoreRegistry = queryableStoreRegistry;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindConsumer(String name, String group,
															KStream<Object, Object> inputTarget,
															ExtendedConsumerProperties<KStreamConsumerProperties> properties) {

		this.KStreamBindingInformationCatalogue.registerConsumerProperties(inputTarget, properties.getExtension());
		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<>(
				properties.getExtension());
		if (properties.getExtension().getSerdeError() == KStreamConsumerProperties.SerdeError.sendToDlq) {
			extendedConsumerProperties.getExtension().setEnableDlq(true);
		}
		if (!StringUtils.hasText(group)) {
			group = binderConfigurationProperties.getApplicationId();
		}
		this.kafkaTopicProvisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);

		//populate the per binding StreamConfig properties
		Map<String, Object> streamConfigGlobalProperties = getApplicationContext().getBean("streamConfigGlobalProperties", Map.class);

		StreamsBuilderFactoryBean streamsBuilder = getApplicationContext().getBean("&stream-builder-" + name, StreamsBuilderFactoryBean.class);

		streamConfigGlobalProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, group);

		if(properties.getExtension().getSerdeError() == KStreamConsumerProperties.SerdeError.logAndContinue) {
			streamConfigGlobalProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueExceptionHandler.class);
		}
		else if(properties.getExtension().getSerdeError() == KStreamConsumerProperties.SerdeError.logAndFail) {
			streamConfigGlobalProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndFailExceptionHandler.class);
		}
		else if (properties.getExtension().getSerdeError() == KStreamConsumerProperties.SerdeError.sendToDlq) {
			streamConfigGlobalProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					SendToDlqAndContinue.class);
		}

		StreamsConfig streamsConfig = new StreamsConfig(streamConfigGlobalProperties) {

			DeserializationExceptionHandler deserializationExceptionHandler;

			@Override
			@SuppressWarnings("unchecked")
			public <T> T getConfiguredInstance(String key, Class<T> t) {
				if (key.equals(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG)){
					if (deserializationExceptionHandler != null){
						return (T)deserializationExceptionHandler;
					}
					else {
						T t1 = super.getConfiguredInstance(key, t);
						deserializationExceptionHandler = (DeserializationExceptionHandler)t1;
						return t1;
					}
				}
				return super.getConfiguredInstance(key, t);
			}
		};

		ConfigurableListableBeanFactory beanFactory = getApplicationContext().getBeanFactory();
		beanFactory.registerSingleton("streamsConfig-" + name, streamsConfig);
		beanFactory.initializeBean(streamsConfig, "streamsConfig-" + name);

		streamsBuilder.setStreamsConfig(streamsConfig);
		streamsBuilder.start();
		queryableStoreRegistry.registerKafkaStreams(streamsBuilder.getKafkaStreams());

		if (extendedConsumerProperties.getExtension().isEnableDlq()) {
			String dlqName = StringUtils.isEmpty(extendedConsumerProperties.getExtension().getDlqName()) ?
					"error." + name + "." + group : extendedConsumerProperties.getExtension().getDlqName();
			KStreamDlqDispatch kStreamDlqDispatch = new KStreamDlqDispatch(dlqName, binderConfigurationProperties,
					extendedConsumerProperties.getExtension());
			SendToDlqAndContinue sendToDlqAndContinue = this.getApplicationContext().getBean(SendToDlqAndContinue.class);
			sendToDlqAndContinue.addKStreamDlqDispatch(name, kStreamDlqDispatch);

			DeserializationExceptionHandler deserializationExceptionHandler = streamsConfig.defaultDeserializationExceptionHandler();
			if(deserializationExceptionHandler instanceof SendToDlqAndContinue) {
				((SendToDlqAndContinue)deserializationExceptionHandler).addKStreamDlqDispatch(name, kStreamDlqDispatch);
			}
		}
		return new DefaultBinding<>(name, group, inputTarget, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KStream<Object, Object>> doBindProducer(String name, KStream<Object, Object> outboundBindTarget,
															ExtendedProducerProperties<KStreamProducerProperties> properties) {
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
			kStreamBoundMessageConversionDelegate.serializeOnOutbound(outboundBindTarget)
					.to(name, Produced.with(keySerde, valueSerde));
		}
		else {
			outboundBindTarget.to(name, Produced.with(keySerde, valueSerde));
		}
	}

	@Override
	public KStreamConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KStreamProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	public void setkStreamExtendedBindingProperties(KStreamExtendedBindingProperties kStreamExtendedBindingProperties) {
		this.kStreamExtendedBindingProperties = kStreamExtendedBindingProperties;
	}
}
