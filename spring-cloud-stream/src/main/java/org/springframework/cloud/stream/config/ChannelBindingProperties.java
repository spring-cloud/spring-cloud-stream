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

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 */
@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
public class ChannelBindingProperties {

	public static final String PATH = "path";

	private Properties consumerProperties = new Properties();

	private Properties producerProperties = new Properties();

	private Map<String,Object> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	public Properties getConsumerProperties() {
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

	public String getBindingPath(String channelName) {
		Object binding = bindings.get(channelName);
		// we may shortcut directly to the path
		if (binding != null) {
			if (binding instanceof String) {
				return (String) binding;
			}
			else if (binding instanceof Map) {
				Map<?, ?> bindingProperties = (Map<?, ?>) binding;
				Object bindingPath = bindingProperties.get(PATH);
				if (bindingPath != null) {
					return bindingPath.toString();
				}
			}
		}
		// just return the channel name if not found
		return channelName;
	}

	public String getTapChannelName(String channelName) {
		return "tap:" + getBindingPath(channelName);
	}
	
}
