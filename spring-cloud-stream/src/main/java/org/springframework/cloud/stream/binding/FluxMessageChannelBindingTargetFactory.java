/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.FluxMessageChannel;

/**
 * @author Soby Chacko
 * @since 4.0.0
 */
public class FluxMessageChannelBindingTargetFactory extends AbstractBindingTargetFactory<FluxMessageChannel> {

	private final MessageChannelConfigurer messageChannelConfigurer;

	private final GenericApplicationContext context;

	public FluxMessageChannelBindingTargetFactory(MessageChannelConfigurer messageChannelConfigurer,
												GenericApplicationContext context) {
		super(FluxMessageChannel.class);
		this.messageChannelConfigurer = messageChannelConfigurer;
		this.context = context;
	}

	@Override
	public FluxMessageChannel createInput(String name) {
		FluxMessageChannel fluxMessageChannel = fluxMessageChannel(name);
		this.messageChannelConfigurer.configureInputChannel(fluxMessageChannel, name);
		return fluxMessageChannel;
	}

	@Override
	public FluxMessageChannel createOutput(String name) {
		FluxMessageChannel fluxMessageChannel = fluxMessageChannel(name);
		this.messageChannelConfigurer.configureOutputChannel(fluxMessageChannel, name);
		return fluxMessageChannel;
	}

	public FluxMessageChannel fluxMessageChannel(String name) {
		FluxMessageChannel fluxMessageChannel = null;
		if (context != null && context.containsBean(name)) {
			try {
				fluxMessageChannel = context.getBean(name, FluxMessageChannel.class);
			}
			catch (BeanCreationException e) {
				// ignore
			}
		}
		if (fluxMessageChannel == null) {
			FluxMessageChannel channel = new FluxMessageChannel();
			channel.setComponentName(name);
			if (context != null && !context.containsBean(name)) {
				context.registerBean(name, FluxMessageChannel.class, () -> channel);
			}
			fluxMessageChannel = channel;
		}
		return fluxMessageChannel;
	}
}
