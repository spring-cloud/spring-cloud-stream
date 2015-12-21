/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderPropertyKeys;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Ilayaperumal Gopinathan
 */
@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
public class ChannelBindingServiceProperties {

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex = 0;

	private int instanceCount = 1;

	private Map<String, BindingProperties> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private Map<String, BinderProperties> binders = new HashMap<>();

	private String defaultBinder;

	private String[] dynamicDestinations = new String[0];

	public Map<String, BindingProperties> getBindings() {
		return bindings;
	}

	public void setBindings(Map<String, BindingProperties> bindings) {
		this.bindings = bindings;
	}

	public Map<String, BinderProperties> getBinders() {
		return binders;
	}

	public void setBinders(Map<String, BinderProperties> binders) {
		this.binders = binders;
	}

	public String getDefaultBinder() {
		return defaultBinder;
	}

	public void setDefaultBinder(String defaultBinder) {
		this.defaultBinder = defaultBinder;
	}

	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public int getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

	public String[] getDynamicDestinations() {
		return dynamicDestinations;
	}

	public void setDynamicDestinations(String[] dynamicDestinations) {
		this.dynamicDestinations = dynamicDestinations;
	}

	public String getBindingDestination(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		// we may shortcut directly to the path
		// just return the channel name if not found
		return bindingProperties != null && StringUtils.hasText(bindingProperties.getDestination()) ?
				bindingProperties.getDestination() : channelName;
	}

	/**
	 * Get consumer properties for the given input channel name.
	 *
	 * @param inputChannelName the input channel name
	 * @return merged consumer properties
	 */
	public Properties getConsumerProperties(String inputChannelName) {
		Properties channelConsumerProperties = new Properties();
		BindingProperties bindingProperties = this.bindings.get(inputChannelName);
		if (bindingProperties != null) {
			if (bindingProperties.getConcurrency() != null) {
				channelConsumerProperties.setProperty(BinderPropertyKeys.CONCURRENCY,
						Integer.toString(bindingProperties.getConcurrency()));
			}
			if (bindingProperties.isDurableSubscription() != null) {
				channelConsumerProperties.setProperty(BinderPropertyKeys.DURABLE,
						Boolean.toString(bindingProperties.isDurableSubscription()));
			}
			updateConsumerPartitionProperties(inputChannelName, channelConsumerProperties);
		}
		return channelConsumerProperties;
	}

	/**
	 * Get producer properties for the given output channel name.
	 *
	 * @param outputChannelName the output channel name
	 * @return merged producer properties
	 */
	public Properties getProducerProperties(String outputChannelName) {
		Properties channelProducerProperties = new Properties();
		updateBatchProperties(outputChannelName, channelProducerProperties);
		updateProducerPartitionProperties(outputChannelName, channelProducerProperties);
		return channelProducerProperties;
	}

	private boolean isPartitionedConsumer(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		return bindingProperties != null && bindingProperties.isPartitioned();
	}

	private boolean isPartitionedProducer(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		return (bindingProperties != null && (StringUtils.hasText(bindingProperties.getPartitionKeyExpression())
				|| StringUtils.hasText(bindingProperties.getPartitionKeyExtractorClass())));
	}

	private void updateBatchProperties(String outputChannelName, Properties producerProperties) {
		BindingProperties bindingProperties = this.bindings.get(outputChannelName);
		if (bindingProperties != null) {
			if (bindingProperties.isBatchingEnabled() != null) {
				producerProperties.setProperty(BinderPropertyKeys.BATCHING_ENABLED,
						String.valueOf(bindingProperties.isBatchingEnabled()));
			}
			if (bindingProperties.getBatchSize() != null) {
				producerProperties.setProperty(BinderPropertyKeys.BATCH_SIZE,
						String.valueOf(bindingProperties.getBatchSize()));
			}
			if (bindingProperties.getBatchBufferLimit() != null) {
				producerProperties.setProperty(BinderPropertyKeys.BATCH_BUFFER_LIMIT,
						String.valueOf(bindingProperties.getBatchBufferLimit()));
			}
			if (bindingProperties.getBatchTimeout() != null) {
				producerProperties.setProperty(BinderPropertyKeys.BATCH_TIMEOUT,
						String.valueOf(bindingProperties.getBatchTimeout()));
			}
		}
	}

	private void updateProducerPartitionProperties(String outputChannelName, Properties producerProperties) {
		BindingProperties bindingProperties = this.bindings.get(outputChannelName);
		if (bindingProperties != null) {
			if (isPartitionedProducer(outputChannelName)) {
				if (bindingProperties.getPartitionKeyExpression() != null) {
					producerProperties.setProperty(BinderPropertyKeys.PARTITION_KEY_EXPRESSION,
							bindingProperties.getPartitionKeyExpression());
				}
				if (bindingProperties.getPartitionKeyExtractorClass() != null) {
					producerProperties.setProperty(BinderPropertyKeys.PARTITION_KEY_EXTRACTOR_CLASS,
							bindingProperties.getPartitionKeyExtractorClass());
				}
				if (bindingProperties.getPartitionSelectorClass() != null) {
					producerProperties.setProperty(BinderPropertyKeys.PARTITION_SELECTOR_CLASS,
							bindingProperties.getPartitionSelectorClass());
				}
				if (bindingProperties.getPartitionSelectorExpression() != null) {
					producerProperties.setProperty(BinderPropertyKeys.PARTITION_SELECTOR_EXPRESSION,
							bindingProperties.getPartitionSelectorExpression());
				}
				if (bindingProperties.getPartitionCount() != null) {
					producerProperties.setProperty(BinderPropertyKeys.NEXT_MODULE_COUNT,
							Integer.toString(bindingProperties.getPartitionCount()));
				}
				if (bindingProperties.getNextModuleConcurrency() != null) {
					producerProperties.setProperty(BinderPropertyKeys.NEXT_MODULE_CONCURRENCY,
							Integer.toString(bindingProperties.getNextModuleConcurrency()));
				}
			}
		}
	}

	private void updateConsumerPartitionProperties(String inputChannelName, Properties consumerProperties) {
		BindingProperties bindingProperties = this.bindings.get(inputChannelName);
		if (bindingProperties != null) {
			if (isPartitionedConsumer(inputChannelName)) {
				consumerProperties.setProperty(BinderPropertyKeys.COUNT,
						Integer.toString(getInstanceCount()));
				consumerProperties.setProperty(BinderPropertyKeys.PARTITION_INDEX,
						Integer.toString(getInstanceIndex()));
			}
		}
	}

	public String getBinder(String channelName) {
		if (!bindings.containsKey(channelName)) {
			return null;
		}
		return bindings.get(channelName).getBinder();
	}

	/**
	 * Return configuration properties as Map.
	 * @return map of channel binding configuration properties.
	 */
	public Map<String, Object> asMapProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put("instanceIndex", String.valueOf(getInstanceIndex()));
		properties.put("instanceCount", String.valueOf(getInstanceCount()));
		// Add Bindings properties
		for (Map.Entry<String, BindingProperties> entry : getBindings().entrySet()) {
			properties.put(entry.getKey(), entry.getValue().toString());
		}
		// Add Binder config properties
		for (Map.Entry<String, BinderProperties> entry : binders.entrySet()) {
			properties.put(entry.getKey(), entry.getValue());
		}
		return properties;
	}

}
