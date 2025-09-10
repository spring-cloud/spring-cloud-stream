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

/**
 * A binder that supports pollable message sources.
 *
 * @param <H> the polled consumer handler type.
 * @param <C> the consumer properties type.
 * @author Gary Russell
 * @since 2.0
 *
 */
public interface PollableConsumerBinder<H, C extends ConsumerProperties> {

	/**
	 * Configure a binding for a pollable message source.
	 * @param name the binding name.
	 * @param group the consumer group.
	 * @param inboundBindTarget the binding target.
	 * @param consumerProperties the consumer properties.
	 * @return the binding.
	 */
	default Binding<PollableSource<H>> bindPollableConsumer(String name, String group,
			PollableSource<H> inboundBindTarget, C consumerProperties) {
		throw new UnsupportedOperationException(
				"This binder does not support pollable consumers");
	}

}
