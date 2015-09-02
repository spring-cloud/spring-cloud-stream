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

package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.ServiceActivator;

/**
 * @author Dave Syer
 *
 */
@EnableBinding(Processor.class)
@ConfigurationProperties("module.logging")
public class LoggingTransformer {

	private static Logger logger = LoggerFactory.getLogger(LoggingTransformer.class);

	/**
	 * The name to include in the log message
	 */
	private String name = "logging";

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
	public Object transform(Object payload) {
		logger.info("Transformed by " + this.name + ": " + payload);
		return payload;
	}

}
