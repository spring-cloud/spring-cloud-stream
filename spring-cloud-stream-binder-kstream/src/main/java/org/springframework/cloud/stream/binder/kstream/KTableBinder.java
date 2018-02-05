/*
 * Copyright 2018 the original author or authors.
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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.kstream.config.KStreamBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamConsumerProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kstream.config.KStreamProducerProperties;
import org.springframework.util.StringUtils;

/**
 *
 * @since 2.0.0
 *
 * @author Soby Chacko
 */
public class KTableBinder extends
		AbstractBinder<KTable<Object, Object>, ExtendedConsumerProperties<KStreamConsumerProperties>, ExtendedProducerProperties<KStreamProducerProperties>>
		implements ExtendedPropertiesBinder<KTable<Object, Object>, KStreamConsumerProperties, KStreamProducerProperties> {

	private final KStreamBinderConfigurationProperties binderConfigurationProperties;

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue;

	private KStreamExtendedBindingProperties kStreamExtendedBindingProperties = new KStreamExtendedBindingProperties();

	public KTableBinder(KStreamBinderConfigurationProperties binderConfigurationProperties, KafkaTopicProvisioner kafkaTopicProvisioner,
						KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue) {
		this.binderConfigurationProperties = binderConfigurationProperties;
		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		KStreamBindingInformationCatalogue = kStreamBindingInformationCatalogue;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Binding<KTable<Object, Object>> doBindConsumer(String name, String group, KTable<Object, Object> inputTarget,
															ExtendedConsumerProperties<KStreamConsumerProperties> properties) {
		ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties = new ExtendedConsumerProperties<>(
				properties.getExtension());
		if (binderConfigurationProperties.getSerdeError() == KStreamBinderConfigurationProperties.SerdeError.sendToDlq) {
			extendedConsumerProperties.getExtension().setEnableDlq(true);
		}
		if (!StringUtils.hasText(group)) {
			group = binderConfigurationProperties.getApplicationId();
		}
		this.kafkaTopicProvisioner.provisionConsumerDestination(name, group, extendedConsumerProperties);

		if (extendedConsumerProperties.getExtension().isEnableDlq()) {
			String dlqName = StringUtils.isEmpty(extendedConsumerProperties.getExtension().getDlqName()) ?
					"error." + name + "." + group : extendedConsumerProperties.getExtension().getDlqName();
			KStreamDlqDispatch kStreamDlqDispatch = new KStreamDlqDispatch(dlqName, binderConfigurationProperties,
					extendedConsumerProperties.getExtension());
			SendToDlqAndContinue sendToDlqAndContinue = this.getApplicationContext().getBean(SendToDlqAndContinue.class);
			sendToDlqAndContinue.addKStreamDlqDispatch(name, kStreamDlqDispatch);

			StreamsConfig streamsConfig = this.KStreamBindingInformationCatalogue.getStreamsConfig(inputTarget);
			DeserializationExceptionHandler deserializationExceptionHandler = streamsConfig.defaultDeserializationExceptionHandler();
			if(deserializationExceptionHandler instanceof SendToDlqAndContinue) {
				((SendToDlqAndContinue)deserializationExceptionHandler).addKStreamDlqDispatch(name, kStreamDlqDispatch);
			}
		}
		return new DefaultBinding<>(name, group, inputTarget, null);
	}

	@Override
	protected Binding<KTable<Object, Object>> doBindProducer(String name, KTable<Object, Object> outboundBindTarget,
															ExtendedProducerProperties<KStreamProducerProperties> properties) {
		throw new UnsupportedOperationException("No producer level binding is allowed for KTable");
	}

	@Override
	public KStreamConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KStreamProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kStreamExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}
}
