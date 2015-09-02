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

package config.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.cloud.stream.annotation.Sink;
import org.springframework.integration.annotation.ServiceActivator;

/**
 * @author Dave Syer
 * @author Marius Bogoevici
 */
@EnableModule(Sink.class)
public class SinkModuleDefinition {

	private static Logger logger = LoggerFactory.getLogger(SinkModuleDefinition.class);

	@ServiceActivator(inputChannel=Sink.INPUT)
	public void loggerSink(Object payload) {
		logger.info("Received: " + payload);
	}

}
