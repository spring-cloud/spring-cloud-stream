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

package org.springframework.cloud.stream.annotation.rxjava;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.cloud.stream.annotation.Processor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;

/**
 * Annotation that identifies the class as RxJava module. The class that has {@link EnableRxJavaModule} annotated is expected
 * to provide a bean that implements {@link RxJavaProcessor}.
 *
 * @author Ilayaperumal Gopinathan
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@EnableModule(Processor.class)
@Import(EnableRxJavaModule.RxJavaMessageHandler.class)
public @interface EnableRxJavaModule {

	@Configuration
	class RxJavaMessageHandler {

		@Autowired
		RxJavaProcessor processor;

		@ServiceActivator(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
		@Bean
		public MessageHandler subjectMessageHandler() {
			SubjectMessageHandler messageHandler = new SubjectMessageHandler(processor);
			messageHandler.setOutputChannelName(Processor.OUTPUT);
			return messageHandler;
		}
	}
}
