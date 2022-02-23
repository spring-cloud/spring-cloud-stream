/*
 * Copyright 2018-2021 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsProducerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Resolver for key and value Serde.
 *
 * On the inbound, if native decoding is enabled, then any deserialization on the value is
 * handled by Kafka. First, we look for any key/value Serde set on the binding itself, if
 * that is not available then look at the common Serde set at the global level. If that
 * fails, it falls back to byte[]. If native decoding is disabled, then the binder will do
 * the deserialization on value and ignore any Serde set for value and rely on the
 * contentType provided. Keys are always deserialized at the broker.
 *
 *
 * Same rules apply on the outbound. If native encoding is enabled, then value
 * serialization is done at the broker using any binder level Serde for value, if not
 * using common Serde, if not, then byte[]. If native encoding is disabled, then the
 * binder will do serialization using a contentType. Keys are always serialized by the
 * broker.
 *
 * For state store, use serdes class specified in
 * {@link org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsStateStore}
 * to create Serde accordingly.
 *
 * @author Soby Chacko
 * @author Lei Chen
 * @author Eduard Domínguez
 */
public class KeyValueSerdeResolver implements ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(KeyValueSerdeResolver.class);

	private final Map<String, Object> streamConfigGlobalProperties;

	private final KafkaStreamsBinderConfigurationProperties binderConfigurationProperties;

	private ConfigurableApplicationContext context;

	KeyValueSerdeResolver(Map<String, Object> streamConfigGlobalProperties,
			KafkaStreamsBinderConfigurationProperties binderConfigurationProperties) {
		this.streamConfigGlobalProperties = streamConfigGlobalProperties;
		this.binderConfigurationProperties = binderConfigurationProperties;
	}

	/**
	 * Provide the {@link Serde} for inbound key.
	 * @param extendedConsumerProperties binding level extended
	 * {@link KafkaStreamsConsumerProperties}
	 * @return configurd {@link Serde} for the inbound key.
	 */
	public Serde<?> getInboundKeySerde(
			KafkaStreamsConsumerProperties extendedConsumerProperties) {
		String keySerdeString = extendedConsumerProperties.getKeySerde();

		return getKeySerde(keySerdeString, extendedConsumerProperties.getConfiguration());
	}

	public Serde<?> getInboundKeySerde(
			KafkaStreamsConsumerProperties extendedConsumerProperties, ResolvableType resolvableType) {
		String keySerdeString = extendedConsumerProperties.getKeySerde();

		return getKeySerde(keySerdeString, resolvableType, extendedConsumerProperties.getConfiguration());
	}

	/**
	 * Provide the {@link Serde} for inbound value.
	 * @param consumerProperties {@link ConsumerProperties} on binding
	 * @param extendedConsumerProperties binding level extended
	 * {@link KafkaStreamsConsumerProperties}
	 * @return configurd {@link Serde} for the inbound value.
	 */
	public Serde<?> getInboundValueSerde(ConsumerProperties consumerProperties,
			KafkaStreamsConsumerProperties extendedConsumerProperties) {
		Serde<?> valueSerde;

		String valueSerdeString = extendedConsumerProperties.getValueSerde();
		try {
			if (consumerProperties != null && consumerProperties.isUseNativeDecoding()) {
				valueSerde = getValueSerde(valueSerdeString, extendedConsumerProperties.getConfiguration());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return valueSerde;
	}

	public Serde<?> getInboundValueSerde(ConsumerProperties consumerProperties,
										KafkaStreamsConsumerProperties extendedConsumerProperties,
										ResolvableType resolvableType) {
		Serde<?> valueSerde;

		String valueSerdeString = extendedConsumerProperties.getValueSerde();
		try {
			if (consumerProperties != null && consumerProperties.isUseNativeDecoding()) {
				valueSerde = getValueSerde(valueSerdeString, resolvableType, extendedConsumerProperties.getConfiguration());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return valueSerde;
	}

	/**
	 * Provide the {@link Serde} for outbound key.
	 * @param properties binding level extended {@link KafkaStreamsProducerProperties}
	 * @return configurd {@link Serde} for the outbound key.
	 */
	public Serde<?> getOuboundKeySerde(KafkaStreamsProducerProperties properties) {
		return getKeySerde(properties.getKeySerde(), properties.getConfiguration());
	}

	public Serde<?> getOuboundKeySerde(KafkaStreamsProducerProperties properties, ResolvableType resolvableType) {
		return getKeySerde(properties.getKeySerde(), resolvableType, properties.getConfiguration());
	}


	/**
	 * Provide the {@link Serde} for outbound value.
	 * @param producerProperties {@link ProducerProperties} on binding
	 * @param kafkaStreamsProducerProperties binding level extended
	 * {@link KafkaStreamsProducerProperties}
	 * @return configurd {@link Serde} for the outbound value.
	 */
	public Serde<?> getOutboundValueSerde(ProducerProperties producerProperties,
			KafkaStreamsProducerProperties kafkaStreamsProducerProperties) {
		Serde<?> valueSerde;
		try {
			if (producerProperties.isUseNativeEncoding()) {
				valueSerde = getValueSerde(
						kafkaStreamsProducerProperties.getValueSerde(), kafkaStreamsProducerProperties.getConfiguration());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return valueSerde;
	}

	public Serde<?> getOutboundValueSerde(ProducerProperties producerProperties,
										KafkaStreamsProducerProperties kafkaStreamsProducerProperties, ResolvableType resolvableType) {
		Serde<?> valueSerde;
		try {
			if (producerProperties.isUseNativeEncoding()) {
				valueSerde = getValueSerde(
						kafkaStreamsProducerProperties.getValueSerde(), resolvableType, kafkaStreamsProducerProperties.getConfiguration());
			}
			else {
				valueSerde = Serdes.ByteArray();
			}
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return valueSerde;
	}

	/**
	 * Provide the {@link Serde} for state store.
	 * @param keySerdeString serde class used for key
	 * @return {@link Serde} for the state store key.
	 */
	public Serde<?> getStateStoreKeySerde(String keySerdeString) {
		return getKeySerde(keySerdeString, (Map<String, ?>) null);
	}

	/**
	 * Provide the {@link Serde} for state store value.
	 * @param valueSerdeString serde class used for value
	 * @return {@link Serde} for the state store value.
	 */
	public Serde<?> getStateStoreValueSerde(String valueSerdeString) {
		try {
			return getValueSerde(valueSerdeString, (Map<String, ?>) null);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
	}

	private Serde<?> getKeySerde(String keySerdeString, Map<String, ?> extendedConfiguration) {
		Serde<?> keySerde;
		try {
			if (StringUtils.hasText(keySerdeString)) {
				keySerde = Utils.newInstance(keySerdeString, Serde.class);
			}
			else {
				keySerde = getFallbackSerde("default.key.serde");
			}
			keySerde.configure(combineStreamConfigProperties(extendedConfiguration), true);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return keySerde;
	}

	private Serde<?> getKeySerde(String keySerdeString, ResolvableType resolvableType, Map<String, ?> extendedConfiguration) {
		Serde<?> keySerde = null;
		try {
			if (StringUtils.hasText(keySerdeString)) {
				keySerde = Utils.newInstance(keySerdeString, Serde.class);
			}
			else {
				if (resolvableType != null &&
						(isResolvalbeKafkaStreamsType(resolvableType) || isResolvableKStreamArrayType(resolvableType))) {
					ResolvableType generic = resolvableType.isArray() ? resolvableType.getComponentType().getGeneric(0) : resolvableType.getGeneric(0);
					Serde<?> fallbackSerde = getFallbackSerde("default.key.serde");
					keySerde = getSerde(generic, fallbackSerde);
				}
				if (keySerde == null) {
					keySerde = Serdes.ByteArray();
				}
			}
			keySerde.configure(combineStreamConfigProperties(extendedConfiguration), true);
		}
		catch (ClassNotFoundException ex) {
			throw new IllegalStateException("Serde class not found: ", ex);
		}
		return keySerde;
	}

	private boolean isResolvableKStreamArrayType(ResolvableType resolvableType) {
		return resolvableType.isArray() &&
				KStream.class.isAssignableFrom(resolvableType.getComponentType().getRawClass());
	}

	private boolean isResolvalbeKafkaStreamsType(ResolvableType resolvableType) {
		return resolvableType.getRawClass() != null && (KStream.class.isAssignableFrom(resolvableType.getRawClass()) || KTable.class.isAssignableFrom(resolvableType.getRawClass()) ||
				GlobalKTable.class.isAssignableFrom(resolvableType.getRawClass()));
	}

	private Serde<?> getSerde(ResolvableType generic, Serde<?> fallbackSerde) {
		Serde<?> serde = null;

		Map<String, Serde> beansOfType = context.getBeansOfType(Serde.class);
		Serde<?>[] serdeBeans = new Serde<?>[1];

		final Class<?> genericRawClazz = generic.getRawClass();
		beansOfType.forEach((k, v) -> {
			final Class<?> classObj = ClassUtils.resolveClassName(((AnnotatedBeanDefinition)
							context.getBeanFactory().getBeanDefinition(k))
							.getMetadata().getClassName(),
					ClassUtils.getDefaultClassLoader());
			try {
				Method[] methods = classObj.getMethods();
				Optional<Method> serdeBeanMethod = Arrays.stream(methods).filter(m -> m.getName().equals(k)).findFirst();
				if (serdeBeanMethod.isPresent()) {
					Method method = serdeBeanMethod.get();
					ResolvableType resolvableType = ResolvableType.forMethodReturnType(method, classObj);
					ResolvableType serdeBeanGeneric = resolvableType.getGeneric(0);
					Class<?> serdeGenericRawClazz = serdeBeanGeneric.getRawClass();
					if (serdeGenericRawClazz != null && genericRawClazz != null) {
						if (serdeGenericRawClazz.isAssignableFrom(genericRawClazz)) {
							serdeBeans[0] = v;
						}
					}
				}
			}
			catch (Exception e) {
				// Pass through...
			}

		});

		if (serdeBeans[0] != null) {
			return serdeBeans[0];
		}

		if (genericRawClazz != null) {
			if (Integer.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.Integer();
			}
			else if (Long.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.Long();
			}
			else if (Short.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.Short();
			}
			else if (Double.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.Double();
			}
			else if (Float.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.Float();
			}
			else if (byte[].class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.ByteArray();
			}
			else if (String.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.String();
			}
			else if (UUID.class.isAssignableFrom(genericRawClazz)) {
				serde = Serdes.UUID();
			}
			else if (!isSerdeFromStandardDefaults(fallbackSerde)) {
				//User purposely set a default serde that is not one of the above
				serde = fallbackSerde;
			}
			else {
				// If the type is Object, then skip assigning the JsonSerde and let the fallback mechanism takes precedence.
				if (!genericRawClazz.isAssignableFrom((Object.class))) {
					serde = new JsonSerde(genericRawClazz);
				}
			}
		}
		return serde;
	}

	private boolean isSerdeFromStandardDefaults(Serde<?> serde) {
		if (serde != null) {
			if (Number.class.isAssignableFrom(serde.getClass())) {
				return true;
			}
			else if (Serdes.ByteArray().getClass().isAssignableFrom(serde.getClass())) {
				return true;
			}
			else if (Serdes.String().getClass().isAssignableFrom(serde.getClass())) {
				return true;
			}
			else if (Serdes.UUID().getClass().isAssignableFrom(serde.getClass())) {
				return true;
			}
		}
		return false;
	}


	private Serde<?> getValueSerde(String valueSerdeString, Map<String, ?> extendedConfiguration)
			throws ClassNotFoundException {
		Serde<?> valueSerde;
		if (StringUtils.hasText(valueSerdeString)) {
			valueSerde = Utils.newInstance(valueSerdeString, Serde.class);
		}
		else {
			valueSerde = getFallbackSerde("default.value.serde");
		}
		valueSerde.configure(combineStreamConfigProperties(extendedConfiguration), false);
		return valueSerde;
	}

	private Serde<?> getFallbackSerde(String s) throws ClassNotFoundException {
		return this.binderConfigurationProperties.getConfiguration()
				.containsKey(s)
				? Utils.newInstance(this.binderConfigurationProperties
						.getConfiguration().get(s),
				Serde.class)
				: Serdes.ByteArray();
	}

	@SuppressWarnings("unchecked")
	private Serde<?> getValueSerde(String valueSerdeString, ResolvableType resolvableType, Map<String, ?> extendedConfiguration)
			throws ClassNotFoundException {
		Serde<?> valueSerde = null;
		if (StringUtils.hasText(valueSerdeString)) {
			valueSerde = Utils.newInstance(valueSerdeString, Serde.class);
		}
		else {

			if (resolvableType != null && ((isResolvalbeKafkaStreamsType(resolvableType)) ||
					(isResolvableKStreamArrayType(resolvableType)))) {
				Serde<?> fallbackSerde = getFallbackSerde("default.value.serde");
				ResolvableType generic = resolvableType.isArray() ? resolvableType.getComponentType().getGeneric(1) : resolvableType.getGeneric(1);
				valueSerde = getSerde(generic, fallbackSerde);
			}
			if (valueSerde == null) {

				valueSerde = Serdes.ByteArray();
			}
		}
		valueSerde.configure(combineStreamConfigProperties(extendedConfiguration), false);
		return valueSerde;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		context = (ConfigurableApplicationContext) applicationContext;
	}

	private Map<String, ?> combineStreamConfigProperties(Map<String, ?> extendedConfiguration) {
		if (extendedConfiguration != null && !extendedConfiguration.isEmpty()) {
			Map<String, Object> streamConfiguration = new HashMap(this.streamConfigGlobalProperties);
			streamConfiguration.putAll(extendedConfiguration);
			return streamConfiguration;
		}
		else {
			return this.streamConfigGlobalProperties;
		}
	}
}
