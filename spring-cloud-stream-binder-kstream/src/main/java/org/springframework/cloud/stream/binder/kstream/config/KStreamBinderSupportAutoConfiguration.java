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

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kstream.KStreamBoundElementFactory;
import org.springframework.cloud.stream.binder.kstream.KStreamListenerParameterAdapter;
import org.springframework.cloud.stream.binder.kstream.KStreamStreamListenerResultAdapter;
import org.springframework.cloud.stream.binder.kstream.MessageConversionDelegate;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public class KStreamBinderSupportAutoConfiguration {

	@Bean
	@ConfigurationProperties(prefix = "spring.cloud.stream.kstream.binder")
	public KafkaBinderConfigurationProperties binderConfigurationProperties() {
		return new KafkaBinderConfigurationProperties();
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
	public StreamsBuilderFactoryBean defaultKafkaStreamBuilder(
			@Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME) ObjectProvider<StreamsConfig> streamsConfigProvider) {
		StreamsConfig streamsConfig = streamsConfigProvider.getIfAvailable();
		if (streamsConfig != null) {
			StreamsBuilderFactoryBean kStreamBuilderFactoryBean = new StreamsBuilderFactoryBean(streamsConfig);
			kStreamBuilderFactoryBean.setPhase(Integer.MAX_VALUE - 500);
			return kStreamBuilderFactoryBean;
		} else {
			throw new UnsatisfiedDependencyException(KafkaStreamsDefaultConfiguration.class.getName(),
					KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME, "streamsConfig",
					"There is no '" + KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME
							+ "' StreamsConfig bean in the application context.\n");
		}
	}

	@Bean(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig streamsConfig(KafkaBinderConfigurationProperties binderConfigurationProperties) {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, binderConfigurationProperties.getKafkaConnectionString());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "default");
		if (!ObjectUtils.isEmpty(binderConfigurationProperties.getConfiguration())) {
			props.putAll(binderConfigurationProperties.getConfiguration());
		}
		return new StreamsConfig(props);
	}

	@Bean
	public KStreamStreamListenerResultAdapter kafkaStreamStreamListenerResultAdapter() {
		return new KStreamStreamListenerResultAdapter();
	}

	@Bean
	public KStreamListenerParameterAdapter kafkaStreamListenerParameterAdapter(
			MessageConversionDelegate messageConversionDelegate) {
		return new KStreamListenerParameterAdapter(messageConversionDelegate);
	}

	@Bean
	public MessageConversionDelegate messageConversionDelegate(BindingServiceProperties bindingServiceProperties,
															CompositeMessageConverterFactory compositeMessageConverterFactory) {
		return new MessageConversionDelegate(bindingServiceProperties, compositeMessageConverterFactory);
	}

	@Bean
	public KStreamBoundElementFactory kafkaStreamBindableTargetFactory(StreamsBuilder kStreamBuilder,
																	BindingServiceProperties bindingServiceProperties) {
		return new KStreamBoundElementFactory(kStreamBuilder, bindingServiceProperties);
	}

}
