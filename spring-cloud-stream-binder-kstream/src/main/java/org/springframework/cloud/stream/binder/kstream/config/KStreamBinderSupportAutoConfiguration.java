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

package org.springframework.cloud.stream.binder.kstream.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.KStreamBindingInformationCatalogue;
import org.springframework.cloud.stream.binder.kstream.KStreamBoundElementFactory;
import org.springframework.cloud.stream.binder.kstream.KStreamBoundMessageConversionDelegate;
import org.springframework.cloud.stream.binder.kstream.KStreamListenerParameterAdapter;
import org.springframework.cloud.stream.binder.kstream.KStreamListenerSetupMethodOrchestrator;
import org.springframework.cloud.stream.binder.kstream.KStreamStreamListenerResultAdapter;
import org.springframework.cloud.stream.binder.kstream.KTableBoundElementFactory;
import org.springframework.cloud.stream.binder.kstream.KeyValueSerdeResolver;
import org.springframework.cloud.stream.binder.kstream.QueryableStoreRegistry;
import org.springframework.cloud.stream.binder.kstream.SendToDlqAndContinue;
import org.springframework.cloud.stream.binder.kstream.StreamsBuildersLifecycle;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
@EnableConfigurationProperties(KStreamExtendedBindingProperties.class)
public class KStreamBinderSupportAutoConfiguration {

	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kstream.binder")
	public KStreamBinderConfigurationProperties binderConfigurationProperties() {
		return new KStreamBinderConfigurationProperties();
	}

	@Bean("streamConfigGlobalProperties")
	public Map<String,Object> streamConfigGlobalProperties(KStreamBinderConfigurationProperties binderConfigurationProperties){
		Map<String,Object> props = new HashMap<>();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, binderConfigurationProperties.getKafkaConnectionString());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, binderConfigurationProperties.getApplicationId());

		if(binderConfigurationProperties.getSerdeError() == KStreamBinderConfigurationProperties.SerdeError.logAndContinue) {
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndContinueExceptionHandler.class);
		}
		else if(binderConfigurationProperties.getSerdeError() == KStreamBinderConfigurationProperties.SerdeError.logAndFail) {
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					LogAndFailExceptionHandler.class);
		}
		else if (binderConfigurationProperties.getSerdeError() == KStreamBinderConfigurationProperties.SerdeError.sendToDlq) {
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
					SendToDlqAndContinue.class);
		}

		if (!ObjectUtils.isEmpty(binderConfigurationProperties.getConfiguration())) {
			props.putAll(binderConfigurationProperties.getConfiguration());
		}

		return props;
	}

	@Bean
	public KStreamStreamListenerResultAdapter kafkaStreamStreamListenerResultAdapter() {
		return new KStreamStreamListenerResultAdapter();
	}

	@Bean
	public KStreamListenerParameterAdapter kafkaStreamListenerParameterAdapter(
			KStreamBoundMessageConversionDelegate kstreamBoundMessageConversionDelegate, KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue) {
		return new KStreamListenerParameterAdapter(kstreamBoundMessageConversionDelegate, KStreamBindingInformationCatalogue);
	}

	@Bean
	public KStreamListenerSetupMethodOrchestrator kStreamListenerSetupMethodOrchestrator(
			BindingServiceProperties bindingServiceProperties,
			KStreamExtendedBindingProperties kStreamExtendedBindingProperties,
			KeyValueSerdeResolver keyValueSerdeResolver,
			KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue,
			KStreamListenerParameterAdapter kafkaStreamListenerParameterAdapter,
			Collection<StreamListenerResultAdapter> streamListenerResultAdapters,
			KStreamBinderConfigurationProperties binderConfigurationProperties) {
		return new KStreamListenerSetupMethodOrchestrator(bindingServiceProperties,
				kStreamExtendedBindingProperties, keyValueSerdeResolver, kStreamBindingInformationCatalogue,
				kafkaStreamListenerParameterAdapter, streamListenerResultAdapters, binderConfigurationProperties);
	}

	@Bean
	public KStreamBoundMessageConversionDelegate messageConversionDelegate(CompositeMessageConverterFactory compositeMessageConverterFactory,
																		SendToDlqAndContinue sendToDlqAndContinue,
																		KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue,
																		KStreamBinderConfigurationProperties binderConfigurationProperties) {
		return new KStreamBoundMessageConversionDelegate(compositeMessageConverterFactory, sendToDlqAndContinue,
				KStreamBindingInformationCatalogue, binderConfigurationProperties);
	}

	@Bean
	public KStreamBoundElementFactory kafkaStreamBindableTargetFactory(BindingServiceProperties bindingServiceProperties,
																	KStreamBindingInformationCatalogue KStreamBindingInformationCatalogue) {
		return new KStreamBoundElementFactory(bindingServiceProperties,
				KStreamBindingInformationCatalogue);
	}

	@Bean
	public KTableBoundElementFactory kTableBoundElementFactory() {
		return new KTableBoundElementFactory();
	}

	@Bean
	public SendToDlqAndContinue kStreamDlqSender() {
		return new SendToDlqAndContinue();
	}

	@Bean
	public KStreamBindingInformationCatalogue boundedKStreamRegistryService() {
		return new KStreamBindingInformationCatalogue();
	}

	@Bean
	@SuppressWarnings("unchecked")
	public KeyValueSerdeResolver keyValueSerdeResolver(@Qualifier("streamConfigGlobalProperties") Object streamConfigGlobalProperties,
													KStreamBinderConfigurationProperties kStreamBinderConfigurationProperties) {
		return new KeyValueSerdeResolver((Map<String,Object>)streamConfigGlobalProperties, kStreamBinderConfigurationProperties);
	}

	@Bean
	public QueryableStoreRegistry queryableStoreTypeRegistry() {
		return new QueryableStoreRegistry();
	}

	@Bean
	public StreamsBuildersLifecycle streamsBuildersLifecycle(KStreamBindingInformationCatalogue kStreamBindingInformationCatalogue,
															QueryableStoreRegistry queryableStoreRegistry){
		return new StreamsBuildersLifecycle(kStreamBindingInformationCatalogue, queryableStoreRegistry);
	}

}
