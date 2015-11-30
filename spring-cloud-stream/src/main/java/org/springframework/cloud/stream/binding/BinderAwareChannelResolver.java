/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import java.util.Properties;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.BeanFactoryMessageChannelDestinationResolver;
import org.springframework.messaging.core.DestinationResolutionException;

/**
 * A {@link org.springframework.messaging.core.DestinationResolver} implementation that first checks for any channel
 * whose name begins with a colon in the {@link Binder}.
 * @author Mark Fisher
 * @author Gary Russell
 */
public class BinderAwareChannelResolver extends BeanFactoryMessageChannelDestinationResolver {

	private final BinderFactory<MessageChannel> binderFactory;

	private final Properties producerProperties;

	public BinderAwareChannelResolver(BinderFactory<MessageChannel> binderFactory, Properties producerProperties) {
		this.binderFactory = binderFactory;
		this.producerProperties = producerProperties;
	}

	@Override
	public MessageChannel resolveDestination(String name) {
		MessageChannel channel = null;
		try {
			return super.resolveDestination(name);
		}
		catch (DestinationResolutionException e) {
		}
		if (name.contains(":")) {
			if (binderFactory != null) {
				String[] tokens = name.split(":", 2);
				String transport = null;
				String type;
				if (tokens.length == 2) {
					type = tokens[0];
				}
				else if (tokens.length == 3) {
					transport = tokens[0];
					type = tokens[1];
				}
				else {
					throw new IllegalArgumentException("Unrecognized channel naming scheme: " + name + " , should be" +
							" [<transport>:]<type>:<name>");
				}
				Binder<MessageChannel> binder = binderFactory.getBinder(transport);
				if ("queue".equals(type)) {
					channel = binder.bindDynamicProducer(name, this.producerProperties);
				}
				else if ("topic".equals(type)) {
					channel = binder.bindDynamicPubSubProducer(name, this.producerProperties);
				}
				else {
					throw new IllegalArgumentException("unrecognized channel type: " + type);
				}
			}
		}
		if (channel == null) {
			channel = super.resolveDestination(name);
		}
		return channel;
	}

}
