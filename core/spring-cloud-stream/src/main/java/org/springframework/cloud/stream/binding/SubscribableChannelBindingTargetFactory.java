/*
 * Copyright 2015-present the original author or authors.
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.messaging.SubscribableChannel;

/**
 * An implementation of {@link BindingTargetFactory} for creating
 * {@link SubscribableChannel}s.
 *
 * @author Marius Bogoevici
 * @author David Syer
 * @author Ilayaperumal Gopinathan
 * @author Oleg Zhurakousky
 * @author Byungjun You
 */
public class SubscribableChannelBindingTargetFactory
		extends AbstractBindingTargetFactory<SubscribableChannel> {

	private final MessageChannelConfigurer messageChannelConfigurer;

	@Autowired
	private GenericApplicationContext context;

	public SubscribableChannelBindingTargetFactory(
			MessageChannelConfigurer messageChannelConfigurer) {
		super(SubscribableChannel.class);
		this.messageChannelConfigurer = messageChannelConfigurer;
	}

	@Override
	public SubscribableChannel createInput(String name) {
		SubscribableChannel subscribableChannel = null;
		if (context != null && context.containsBean(name)) {
			try {
				subscribableChannel = context.getBean(name, SubscribableChannel.class);
			}
			catch (BeanCreationException e) {
				// ignore
				/*
				 * Since we still support annotation-based programming model, this exception happens
				 * because of proxies related to @Input @Output
				 */
			}
		}
		if (subscribableChannel == null) {
			DirectWithAttributesChannel channel = new DirectWithAttributesChannel();
			channel.setComponentName(name);
			if (context != null && !context.containsBean(name)) {
				context.registerBean(name, DirectWithAttributesChannel.class, () -> channel);
			}
			subscribableChannel = channel;
		}
		if (subscribableChannel instanceof DirectWithAttributesChannel directWithAttributesChannel) {
			directWithAttributesChannel.setAttribute("type", "input");
			this.messageChannelConfigurer.configureInputChannel(directWithAttributesChannel, name);
		}

		return subscribableChannel;
	}

	@Override
	public SubscribableChannel createOutput(String name) {
		SubscribableChannel subscribableChannel = null;
		if (context != null && context.containsBean(name)) {
			try {
				subscribableChannel = context.getBean(name, SubscribableChannel.class);
			}
			catch (BeanCreationException e) {
				// ignore
			}
		}
		if (subscribableChannel == null) {
			DirectWithAttributesChannel channel = new DirectWithAttributesChannel();
			channel.setComponentName(name);
			if (context != null && !context.containsBean(name)) {
				context.registerBean(name, DirectWithAttributesChannel.class, () -> channel);
			}
			subscribableChannel = channel;
		}
		if (subscribableChannel instanceof DirectWithAttributesChannel directWithAttributesChannel) {
			directWithAttributesChannel.setAttribute("type", "output");
			this.messageChannelConfigurer.configureOutputChannel(directWithAttributesChannel, name);
		}

		return subscribableChannel;
	}

}
