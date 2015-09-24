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
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 */
@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
public class ChannelBindingServiceProperties {

	@Value("${INSTANCE_INDEX:${CF_INSTANCE_INDEX:0}}")
	private int instanceIndex = 0;

	private int instanceCount = 1;

	private Properties consumerProperties = new Properties();

	private Properties producerProperties = new Properties();

	private Map<String,BindingProperties> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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

	public Map<String, BindingProperties> getBindings() {
		return bindings;
	}

	public void setBindings(Map<String, BindingProperties> bindings) {
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
		BindingProperties bindingProperties = bindings.get(channelName);
		// we may shortcut directly to the path
		// just return the channel name if not found
		return bindingProperties != null && StringUtils.hasText(bindingProperties.getDestination()) ?
				bindingProperties.getDestination() : channelName;
	}

	public int getPartitionCount(String channelName) {
		return bindings.containsKey(channelName) ? bindings.get(channelName).getPartitionCount() : 1;
	}

	public boolean isPartitionedConsumer(String channelName) {
		return bindings.containsKey(channelName) && bindings.get(channelName).isPartitioned();
	}

	public boolean isPartitionedProducer(String channelName) {
		BindingProperties bindingProperties = bindings.get(channelName);
		return bindingProperties != null &&
					(StringUtils.hasText(bindingProperties.getPartitionKeyExpression())
					|| StringUtils.hasText(bindingProperties.getPartitionKeyExtractorClass()));

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
			channelConsumerProperties.putAll(consumerProperties);
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
			channelProducerProperties.putAll(this.producerProperties);
			channelProducerProperties.setProperty(BinderProperties.NEXT_MODULE_COUNT,
					Integer.toString(getPartitionCount(outputChannelName)));
			BindingProperties bindingProperties = bindings.get(outputChannelName);
			if (bindingProperties != null) {
				if (bindingProperties.getPartitionKeyExpression() != null) {
					channelProducerProperties.setProperty(BinderProperties.PARTITION_KEY_EXPRESSION,
							bindingProperties.getPartitionKeyExpression());
				}
				if (bindingProperties.getPartitionKeyExtractorClass() != null) {
					channelProducerProperties.setProperty(BinderProperties.PARTITION_KEY_EXTRACTOR_CLASS,
							bindingProperties.getPartitionKeyExtractorClass());
				}
				if (bindingProperties.getPartitionSelectorClass() != null) {
					channelProducerProperties.setProperty(BinderProperties.PARTITION_SELECTOR_CLASS,
							bindingProperties.getPartitionSelectorClass());
				}
				if (bindingProperties.getPartitionSelectorExpression() != null) {
					channelProducerProperties.setProperty(BinderProperties.PARTITION_SELECTOR_EXPRESSION,
							bindingProperties.getPartitionSelectorExpression());
				}
			}
			return channelProducerProperties;
		}
		else {
			return this.producerProperties;
		}
	}

	public String getTapChannelName(String channelName) {
		return "tap:" + getBindingDestination(channelName);
	}

}
