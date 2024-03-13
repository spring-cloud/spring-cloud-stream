/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.cloud.stream.binder.pulsar;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.ProducerBuilder;

import org.springframework.cloud.stream.binder.pulsar.properties.ConsumerConfigProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.ProducerConfigProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.StringUtils;

/**
 * Binder utility methods.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
final class PulsarBinderUtils {

	private static final LogAccessor LOGGER = new LogAccessor(PulsarBinderUtils.class);

	private static final String SUBSCRIPTION_NAME_FORMAT_STR = "%s-anon-subscription-%s";

	private PulsarBinderUtils() {
	}

	/**
	 * Gets the subscription name to use for the binder.
	 * @param consumerProps the pulsar consumer props
	 * @param consumerDestination the destination being subscribed to
	 * @return the subscription name from the consumer properties or a generated name in
	 * the format {@link #SUBSCRIPTION_NAME_FORMAT_STR} when the name is not set on the
	 * consumer properties
	 */
	static String subscriptionName(PulsarConsumerProperties consumerProps, ConsumerDestination consumerDestination) {
		if (StringUtils.hasText(consumerProps.getSubscription().getName())) {
			return consumerProps.getSubscription().getName();
		}
		return SUBSCRIPTION_NAME_FORMAT_STR.formatted(consumerDestination.getName(), UUID.randomUUID());
	}

	/**
	 * Merges base and extended producer properties defined at the binder and binding
	 * level.
	 * <p>Only base properties whose value has changed from the default are included in
	 * result map. All extended properties are included, regardless of their value,
	 * which ensures that the extended property defaults are respected.
	 * <p>If a property is defined at both the binder and binding level, the binding level
	 * property value takes precedence.
	 * @param binderProducerProps the binder level producer config properties (eg.
	 * 'spring.cloud.stream.pulsar.binder.producer.*')
	 * @param bindingProducerProps the binding level config properties (eg.
	 * 'spring.cloud.stream.pulsar.bindings.myBinding-out-0.producer.*')
	 * @return map of merged binder and binding producer properties
	 */
	static Map<String, Object> mergeModifiedProducerProperties(ProducerConfigProperties binderProducerProps,
			ProducerConfigProperties bindingProducerProps) {
		// Layer the base props for global -> binder -> bindings
		var globalProducerProps = new ProducerConfigProperties().toBaseProducerPropertiesMap();
		var binderBaseProducerProps = binderProducerProps.toBaseProducerPropertiesMap();
		var bindingBaseProducerProps = bindingProducerProps.toBaseProducerPropertiesMap();
		var layeredBaseProducerProps = PulsarBinderUtils.mergePropertiesWithPrecedence(globalProducerProps,
				binderBaseProducerProps, bindingBaseProducerProps, false);
		// Layer the extended props for global -> binder -> bindings
		var globalExtProducerProps = new ProducerConfigProperties().toExtendedProducerPropertiesMap();
		var binderExtProducerProps = binderProducerProps.toExtendedProducerPropertiesMap();
		var bindingExtProducerProps = bindingProducerProps.toExtendedProducerPropertiesMap();
		var layeredExtProducerProps = PulsarBinderUtils.mergePropertiesWithPrecedence(globalExtProducerProps,
				binderExtProducerProps, bindingExtProducerProps, true);
		// Combine both base and extended layers
		var layeredProducerProps = new HashMap<>(layeredBaseProducerProps);
		layeredProducerProps.putAll(layeredExtProducerProps);
		return layeredProducerProps;
	}

	/**
	 * Merges base and extended consumer properties defined at the binder and binding
	 * level.
	 * <p>Only base properties whose value has changed from the default are included in
	 * result map. All extended properties are included, regardless of their value,
	 * which ensures that the extended property defaults are respected.
	 * <p>If a property is defined at both the binder and binding level, the binding level
	 * property value takes precedence.
	 * @param binderConsumerProps the binder level consumer config properties (eg.
	 * 'spring.cloud.stream.pulsar.binder.consumer.*')
	 * @param bindingConsumerProps the binding level config properties (eg.
	 * 'spring.cloud.stream.pulsar.bindings.myBinding-in-0.consumer.*')
	 * @return map of merged binder and binding consumer properties
	 */
	static Map<String, Object> mergeModifiedConsumerProperties(ConsumerConfigProperties binderConsumerProps,
			ConsumerConfigProperties bindingConsumerProps) {
		// Layer the base props for global -> binder -> bindings
		var globalBaseConsumerProps = new ConsumerConfigProperties().toBaseConsumerPropertiesMap();
		var binderBaseConsumerProps = binderConsumerProps.toBaseConsumerPropertiesMap();
		var bindingBaseConsumerProps = bindingConsumerProps.toBaseConsumerPropertiesMap();
		var layeredBaseConsumerProps = PulsarBinderUtils.mergePropertiesWithPrecedence(globalBaseConsumerProps,
				binderBaseConsumerProps, bindingBaseConsumerProps, false);
		// Layer the extended props for global -> binder -> bindings
		var globalExtConsumerProps = new ConsumerConfigProperties().toExtendedConsumerPropertiesMap();
		var binderExtConsumerProps = binderConsumerProps.toExtendedConsumerPropertiesMap();
		var bindingExtConsumerProps = bindingConsumerProps.toExtendedConsumerPropertiesMap();
		var layeredExtConsumerProps = PulsarBinderUtils.mergePropertiesWithPrecedence(globalExtConsumerProps,
				binderExtConsumerProps, bindingExtConsumerProps, true);
		// Combine both base and extended layers
		var layeredConsumerProps = new HashMap<>(layeredBaseConsumerProps);
		layeredConsumerProps.putAll(layeredExtConsumerProps);
		return layeredConsumerProps;
	}

	/**
	 * Merges properties defined at the binder and binding level with binding properties
	 * overriding binder properties.
	 * <p>
	 * @param globalProps the map of global level properties (eg. 'spring.pulsar.consumer.*')
	 * @param binderProps the map of binder level properties (eg.
	 * 'spring.cloud.stream.pulsar.binder.consumer.*')
	 * @param bindingProps the map of binding level properties (eg.
	 * 'spring.cloud.stream.pulsar.bindings.myBinding-in-0.consumer.*')
	 * @param includeDefaults whether to also include unmodified properties with their default values
	 * @return map of merged binder and binding properties with binding properties overriding binder properties
	 */
	static Map<String, Object> mergePropertiesWithPrecedence(Map<String, Object> globalProps,
			Map<String, Object> binderProps, Map<String, Object> bindingProps, boolean includeDefaults) {
		Objects.requireNonNull(globalProps, "globalProps must be specified");
		Objects.requireNonNull(binderProps, "binderProps must be specified");
		Objects.requireNonNull(bindingProps, "bindingProps must be specified");

		Map<String, Object> newOrModifiedBinderProps = extractNewOrModifiedProperties(binderProps, globalProps);
		LOGGER.trace(() -> "New or modified binder props: %s".formatted(newOrModifiedBinderProps));

		Map<String, Object> newOrModifiedBindingProps = extractNewOrModifiedProperties(bindingProps, globalProps);
		LOGGER.trace(() -> "New or modified binding props: %s".formatted(newOrModifiedBindingProps));

		Map<String, Object> mergedProps = new HashMap<>(newOrModifiedBinderProps);
		mergedProps.putAll(newOrModifiedBindingProps);

		// Add in default properties for any props not customized by the user
		if (includeDefaults) {
			globalProps.forEach(mergedProps::putIfAbsent);
		}
		LOGGER.trace(() -> "Final merged props: %s".formatted(mergedProps));
		return mergedProps;
	}

	private static Map<String, Object> extractNewOrModifiedProperties(Map<String, Object> candidateProps,
			Map<String, Object> baseProps) {
		Map<String, Object> newOrModifiedProps = new HashMap<>();
		candidateProps.forEach((propName, propValue) -> {
			if (!baseProps.containsKey(propName) || (!Objects.equals(propValue, baseProps.get(propName)))) {
				newOrModifiedProps.put(propName, propValue);
			}
		});
		return newOrModifiedProps;
	}

	/**
	 * Configures the specified properties onto the specified builder in a manner that
	 * loads non-serializable properties. See
	 * <a href="https://github.com/apache/pulsar/pull/18344">Pulsar PR</a>.
	 * @param builder the builder
	 * @param properties the properties to set on the builder
	 * @param <T> the payload type
	 */
	static <T> void loadConf(ProducerBuilder<T> builder, Map<String, Object> properties) {
		builder.loadConf(properties);
		// Set fields that are not loaded by loadConf
		if (properties.containsKey("encryptionKeys")) {
			@SuppressWarnings("unchecked")
			Collection<String> keys = (Collection<String>) properties.get("encryptionKeys");
			keys.forEach(builder::addEncryptionKey);
		}
		if (properties.containsKey("customMessageRouter")) {
			builder.messageRouter((MessageRouter) properties.get("customMessageRouter"));
		}
		if (properties.containsKey("batcherBuilder")) {
			builder.batcherBuilder((BatcherBuilder) properties.get("batcherBuilder"));
		}
		if (properties.containsKey("cryptoKeyReader")) {
			builder.cryptoKeyReader((CryptoKeyReader) properties.get("cryptoKeyReader"));
		}
	}
}
