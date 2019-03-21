/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;

/**
 * Automatically registers the {@link MessageCollector} associated with the test binder as
 * a bean.
 *
 * @author Marius Bogoevici
 */
@Configuration
public class MessageCollectorAutoConfiguration {

	@Bean
	public MessageCollector messageCollector(BinderFactory binderFactory) {
		return ((TestSupportBinder) binderFactory.getBinder("test", MessageChannel.class))
				.messageCollector();
	}

}
