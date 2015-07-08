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

package org.springframework.cloud.streams.config;

import java.util.Properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * @author Dave Syer
 *
 */
@ConfigurationProperties("spring.cloud.channels")
@JsonInclude(Include.NON_DEFAULT)
public class ChannelBindingProperties {

	private String outputChannelName = "group.0";

	private String inputChannelName = "group.0";

	private Properties consumerProperties = new Properties();

	private Properties producerProperties = new Properties();

	private boolean autoStartup = true;

	public String getOutputChannelName() {
		return this.outputChannelName;
	}

	public String getInputChannelName() {
		return this.inputChannelName;
	}

	public void setOutputChannelName(String outputChannelName) {
		this.outputChannelName = outputChannelName;
	}

	public void setInputChannelName(String inputChannelName) {
		this.inputChannelName = inputChannelName;
	}

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

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public String getTapChannelName() {
		return getTapChannelName(getOutputChannelName());
	}

	public String getTapChannelName(String prefix) {
		return "tap:" + prefix;
	}

}
