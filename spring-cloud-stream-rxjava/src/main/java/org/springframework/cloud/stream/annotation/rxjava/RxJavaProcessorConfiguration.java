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

package org.springframework.cloud.stream.annotation.rxjava;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;

/**
 * Configuration class for RxJava module support.
 *
 * @author Ilayaperumal Gopinathan
 * @author Marius Bogoevici
 */
@Configuration
@Deprecated
public class RxJavaProcessorConfiguration {

	@Autowired
	RxJavaProcessor processor;

	@ServiceActivator(inputChannel = Processor.INPUT, phase = "0")
	@Bean
	public MessageHandler subjectMessageHandler() {
		SubjectMessageHandler messageHandler = new SubjectMessageHandler(this.processor);
		messageHandler.setOutputChannelName(Processor.OUTPUT);
		return messageHandler;
	}
}
