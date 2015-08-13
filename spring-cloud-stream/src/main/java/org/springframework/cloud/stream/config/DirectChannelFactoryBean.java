/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.config;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

/**
 * {@link FactoryBean} for creating channels for fields annotated with
 * {@link org.springframework.cloud.stream.annotation.Input} and
 * {@link org.springframework.cloud.stream.annotation.Output}.
 *
 * @author Marius Bogoevici
 */
public class DirectChannelFactoryBean implements FactoryBean<MessageChannel> {

	private DirectChannel directChannel;

	@Override
	public synchronized MessageChannel getObject() throws Exception {
		if (directChannel == null) {
			directChannel = new DirectChannel();
		}
		return directChannel;
	}

	@Override
	public Class<?> getObjectType() {
		return DirectChannel.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}
}
