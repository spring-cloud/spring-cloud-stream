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

package org.springframework.cloud.stream.binder;

import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * @param <C> binder type
 * @param <CP> consumer properties type
 * @param <PP> producer properties type
 * @author Gary Russell
 * @since 2.0
 */
// @checkstyle:off
public abstract class AbstractPollableConsumerTestBinder<C extends AbstractBinder<MessageChannel, CP, PP>, CP extends ConsumerProperties, PP extends ProducerProperties>
		extends AbstractTestBinder<C, CP, PP>
		implements PollableConsumerBinder<MessageHandler, CP> {

	// @checkstyle:on

	private PollableConsumerBinder<MessageHandler, CP> binder;

	@SuppressWarnings("unchecked")
	public void setPollableConsumerBinder(
			PollableConsumerBinder<MessageHandler, CP> binder) {
		super.setBinder((C) binder);
		this.binder = binder;
	}

	@Override
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name,
			String group, PollableSource<MessageHandler> inboundBindTarget,
			CP consumerProperties) {
		return this.binder.bindPollableConsumer(name, group, inboundBindTarget,
				consumerProperties);
	}

}
