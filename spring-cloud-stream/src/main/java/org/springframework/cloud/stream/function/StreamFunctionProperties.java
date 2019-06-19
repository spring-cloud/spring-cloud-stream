/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.function;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.Processor;

/**
 * @author Oleg Zhurakousky
 * @author Tolga Kavukcu
 * @since 2.1
 */
@ConfigurationProperties("spring.cloud.stream.function")
public class StreamFunctionProperties {

	/**
	 * Definition of functions to bind. If several functions need to be composed into one,
	 * use pipes (e.g., 'fooFunc\|barFunc')
	 */
	private String definition;

	private BindingServiceProperties bindingServiceProperties;

	private String inputDestinationName = Processor.INPUT;

	private String outputDestinationName = Processor.OUTPUT;

	public String getDefinition() {
		return this.definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

	BindingServiceProperties getBindingServiceProperties() {
		return this.bindingServiceProperties;
	}

	void setBindingServiceProperties(BindingServiceProperties bindingServiceProperties) {
		this.bindingServiceProperties = bindingServiceProperties;
	}

	String getInputDestinationName() {
		return this.inputDestinationName;
	}

	void setInputDestinationName(String inputDestinationName) {
		this.inputDestinationName = inputDestinationName;
	}

	String getOutputDestinationName() {
		return this.outputDestinationName;
	}

	void setOutputDestinationName(String outputDestinationName) {
		this.outputDestinationName = outputDestinationName;
	}

}
