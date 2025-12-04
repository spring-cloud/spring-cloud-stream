/*
 * Copyright 2018-present the original author or authors.
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

import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.util.Assert;

/**
 * An implementation of {@link BindingTargetFactory} for creating
 * {@link DefaultPollableMessageSource}s.
 *
 * @author Gary Russell
 */
public class MessageSourceBindingTargetFactory
		extends AbstractBindingTargetFactory<PollableMessageSource> {

	private final MessageChannelAndSourceConfigurer messageSourceConfigurer;

	private final SmartMessageConverter messageConverter;

	public MessageSourceBindingTargetFactory(SmartMessageConverter messageConverter,
			MessageChannelConfigurer messageSourceConfigurer) {
		super(PollableMessageSource.class);
		Assert.isInstanceOf(MessageChannelAndSourceConfigurer.class,
				messageSourceConfigurer);
		this.messageSourceConfigurer = (MessageChannelAndSourceConfigurer) messageSourceConfigurer;
		this.messageConverter = messageConverter;
	}

	@Override
	public PollableMessageSource createInput(String name) {
		DefaultPollableMessageSource binding = new DefaultPollableMessageSource(
				this.messageConverter);
		this.messageSourceConfigurer.configurePolledMessageSource(binding, name);
		return binding;
	}

	@Override
	public PollableMessageSource createOutput(String name) {
		throw new UnsupportedOperationException();
	}

}
