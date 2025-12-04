/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The DltPublishingContext is meant to be used along with {@link DltAwareProcessor}
 * when publishing failed record to a DLT. DltPublishingContext is particularly used
 * for accessing framework beans such as the {@link StreamBridge}.
 *
 * @author Soby Chacko
 * @since 4.1.0
 */
public class DltPublishingContext implements ApplicationContextAware, InitializingBean {

	private ConfigurableApplicationContext applicationContext;

	private StreamBridge streamBridge;

	@Override
	public void afterPropertiesSet() {
		this.streamBridge = applicationContext.getBean(StreamBridge.class);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	public StreamBridge getStreamBridge() {
		return this.streamBridge;
	}
}
