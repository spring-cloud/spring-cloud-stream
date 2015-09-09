/*
 * Copyright 2015 the original author or authors.
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

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderProperties;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 */
@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
public class ChannelBindingProperties {

	public static final String DESTINATION = "destination";

	public static final String PARTITION_COUNT = "partitionCount";

	public static final String PARTITIONED = "partitioned";

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex;

	@Value("${INSTANCE_COUNT:1}")
	private int instanceCount;

	private Properties consumerProperties = new Properties();

	private Properties producerProperties = new Properties();

	private Map<String,Object> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private Properties getConsumerProperties() {
		return this.consumerProperties;
	}

	public void setConsumerProperties(Properties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	public Properties getProducerProperties() {
		return this.producerProperties;
	}

	public void setProducerProperties(Properties producerProperties) {
		this.producerProperties = producerProperties;
	}

	public Map<String, Object> getBindings() {
		return bindings;
	}

	public void setBindings(Map<String, Object> bindings) {
		this.bindings = bindings;
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


	public String getBindingDestination(String channelName) {
		Object binding = bindings.get(channelName);
		// we may shortcut directly to the path
		if (binding != null) {
			if (binding instanceof String) {
				return (String) binding;
			}
			else if (binding instanceof Map) {
				Map<?, ?> bindingProperties = (Map<?, ?>) binding;
				Object bindingPath = bindingProperties.get(DESTINATION);
				if (bindingPath != null) {
					return bindingPath.toString();
				}
			}
		}
		// just return the channel name if not found
		return channelName;
	}

	public int getPartitionCount(String channelName) {
		Object binding = bindings.get(channelName);
		// we may shortcut directly to the path
		if (binding instanceof Map) {
			try {
				Map<?, ?> bindingProperties = (Map<?, ?>) binding;
				Object bindingPath = bindingProperties.get(PARTITION_COUNT);
				if (bindingPath != null) {
					return Integer.parseInt(bindingPath.toString());
				}
			} catch (NumberFormatException e) {
				// ignore and just return 1
			}
		}
		return 1;
	}

	public boolean isPartitionedConsumer(String channelName) {
		Object binding = bindings.get(channelName);
		// if the setting is just a target shortcut
		if (binding == null || binding instanceof String) {
			return false;
		}
		else if (binding instanceof Map) {
			Map<?, ?> bindingProperties = (Map<?, ?>) binding;
			Object bindingPath = bindingProperties.get(PARTITIONED);
			if (bindingPath != null) {
				return Boolean.valueOf(bindingPath.toString());
			}
		}
		// just return the channel name if not found
		return false;
	}

	public boolean isPartitionedProducer(String channelName) {
		Object binding = bindings.get(channelName);
		// if the setting is just a target shortcut
		if (binding == null || binding instanceof String) {
			return false;
		}
		else if (binding instanceof Map) {
			Map<?, ?> bindingProperties = (Map<?, ?>) binding;
			return bindingProperties.get(BinderProperties.PARTITION_KEY_EXPRESSION) != null
					|| bindingProperties.get(BinderProperties.PARTITION_KEY_EXTRACTOR_CLASS) != null;
		}
		return false;
	}

	/**
	 * Merge general properties provided by 'spring.cloud.stream.consumerProperties.*' with individual binding
	 * properties supplied via binders.
	 *
	 * @param inputChannelName the input channel name
	 * @return merged consumer properties
	 */
	public Properties getConsumerProperties(String inputChannelName) {
		if (isPartitionedConsumer(inputChannelName)) {
			Properties channelConsumerProperties = new Properties();
			if (consumerProperties != null) {
				channelConsumerProperties.putAll(consumerProperties);
			}
			channelConsumerProperties.setProperty(BinderProperties.COUNT,
					Integer.toString(getInstanceCount()));
			channelConsumerProperties.setProperty(BinderProperties.PARTITION_INDEX,
					Integer.toString(getInstanceIndex()));
			return channelConsumerProperties;
		}
		else {
			return getConsumerProperties();
		}
	}

	/**
	 * Merge general properties provided by 'spring.cloud.stream.producerProperties.*' with individual binding
	 * properties supplied via binders.
	 *
	 * @param outputChannelName the output channel name
	 * @return merged producer properties
	 */
	public Properties getProducerProperties(String outputChannelName) {
		if (isPartitionedProducer(outputChannelName)) {
			Properties channelProducerProperties = new Properties();
			if (this.producerProperties != null) {
				channelProducerProperties.putAll(this.producerProperties);
			}
			channelProducerProperties.put(BinderProperties.MIN_PARTITION_COUNT,
					Integer.toString(getPartitionCount(outputChannelName)));
			channelProducerProperties.setProperty(BinderProperties.NEXT_MODULE_COUNT,
					Integer.toString(getPartitionCount(outputChannelName)));
			copyChannelBindingProperty(outputChannelName, channelProducerProperties,
					BinderProperties.PARTITION_KEY_EXPRESSION);
			copyChannelBindingProperty(outputChannelName, channelProducerProperties,
					BinderProperties.PARTITION_KEY_EXTRACTOR_CLASS);
			copyChannelBindingProperty(outputChannelName, channelProducerProperties,
					BinderProperties.PARTITION_SELECTOR_CLASS);
			copyChannelBindingProperty(outputChannelName, channelProducerProperties,
					BinderProperties.PARTITION_SELECTOR_EXPRESSION);
			return channelProducerProperties;
		}
		else {
			return this.producerProperties;
		}
	}

	@SuppressWarnings("unchecked")
	private void copyChannelBindingProperty(String outputChannelName, Properties targetProperties, String propertyName) {
		Map<String, Object> channelBindingProperties = (Map<String, Object>) bindings.get(outputChannelName);
		if (null != channelBindingProperties && channelBindingProperties.containsKey(propertyName)) {
			targetProperties.setProperty(propertyName, (String) channelBindingProperties.get(propertyName));
		}
	}

	public String getTapChannelName(String channelName) {
		return "tap:" + getBindingDestination(channelName);
	}

}
