/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.cloud.binder.apps;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.SendTo;

@SpringBootApplication()//(excludeName="org.springframework.cloud.binder.apps.StreamListenerExpectingStation")
@EnableBinding(Processor.class)
@ConditionalOnProperty(havingValue="true", name="enable-employee")
public class StreamListenerExpectingEmployee {

	public static final String replyChannelName = "reply";

	@StreamListener(Processor.INPUT)
	@SendTo(replyChannelName)
	public Employee transform(Employee in) {
		System.setProperty("enable-employee", "false");
		return in;
	}
}
