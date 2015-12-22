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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
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

	private static final String PARTITIONED = "partitioned";

	private static final String PARTITION_COUNT = "partitionCount";

	private static final String PARTITION_KEY_EXPRESSION = "partitionKeyExpression";

	private static final String PARTITION_KEY_EXTRACTOR_CLASS = "partitionKeyExtractorClass";

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex = 0;

	private int instanceCount = 1;

	private Map<String, BindingProperties> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	private Map<String, BinderProperties> binders = new HashMap<>();

	private String defaultBinder;

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

	public String getBindingDestination(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		// we may shortcut directly to the path
		// just return the channel name if not found
		return bindingProperties != null && StringUtils.hasText(bindingProperties.getDestination()) ?
				bindingProperties.getDestination() : channelName;
	}

	public int getPartitionCount(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		if (bindingProperties != null && bindingProperties.getConfig() != null) {
			return bindingProperties.getConfig().containsKey(PARTITION_COUNT) ?
					Integer.valueOf(bindingProperties.getConfig().get(PARTITION_COUNT)) : 1;
		}
		return 1;
	}

	public boolean isPartitionedConsumer(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		if (bindingProperties != null && bindingProperties.getConfig() != null) {
			return bindingProperties.getConfig().containsKey(PARTITIONED) ?
					Boolean.valueOf(bindingProperties.getConfig().get(PARTITIONED)) : false;
		}
		return false;
	}

	public boolean isPartitionedProducer(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		if (bindingProperties == null) {
			return false;
		}
		Map<String, String> binderProperties = bindingProperties.getConfig();
		return binderProperties != null &&
				(StringUtils.hasText(binderProperties.get(PARTITION_KEY_EXPRESSION))
						|| StringUtils.hasText(binderProperties.get(PARTITION_KEY_EXTRACTOR_CLASS)));

	}

	/**
	 * Merge general properties provided by 'spring.cloud.stream.consumerProperties.*' with individual binding
	 * properties supplied via binders.
	 *
	 * @param inputChannelName the input channel name
	 * @return merged consumer properties
	 */
	public Properties getConsumerProperties(String inputChannelName) {
		Properties channelConsumerProperties = new Properties();
		Map<String, String> perBindingBinderProperties = getPerBindingProperties(inputChannelName);
		if (perBindingBinderProperties != null) {
			channelConsumerProperties.putAll(perBindingBinderProperties);
		}
		if (isPartitionedConsumer(inputChannelName)) {
			channelConsumerProperties.setProperty(org.springframework.cloud.stream.binder.BinderProperties.COUNT,
					Integer.toString(getInstanceCount()));
			channelConsumerProperties.setProperty(org.springframework.cloud.stream.binder.BinderProperties.PARTITION_INDEX,
					Integer.toString(getInstanceIndex()));
		}
		return channelConsumerProperties;
	}

	/**
	 * Merge general properties provided by 'spring.cloud.stream.producerProperties.*' with individual binding
	 * properties supplied via binders.
	 *
	 * @param outputChannelName the output channel name
	 * @return merged producer properties
	 */
	public Properties getProducerProperties(String outputChannelName) {
		Properties channelProducerProperties = new Properties();
		Map<String, String> perBindingBinderProperties = getPerBindingProperties(outputChannelName);
		if (perBindingBinderProperties != null) {
			channelProducerProperties.putAll(perBindingBinderProperties);
		}
		if (isPartitionedProducer(outputChannelName)) {
			channelProducerProperties.remove(PARTITION_COUNT);
			channelProducerProperties.setProperty(org.springframework.cloud.stream.binder.BinderProperties.NEXT_MODULE_COUNT,
					Integer.toString(getPartitionCount(outputChannelName)));
		}
		return channelProducerProperties;
	}

	private Map<String, String> getPerBindingProperties(String channelName) {
		BindingProperties properties = bindings.get(channelName);
		return (properties != null) ? properties.getConfig() : null;
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
		for (Map.Entry<String, BindingProperties> entry : getBindings().entrySet()) {
			properties.put(entry.getKey(), entry.getValue().toString());
		}
		for (Map.Entry<String, BinderProperties> entry : binders.entrySet()) {
			properties.put(entry.getKey(), entry.getValue());
		}
		return properties;
	}

}
