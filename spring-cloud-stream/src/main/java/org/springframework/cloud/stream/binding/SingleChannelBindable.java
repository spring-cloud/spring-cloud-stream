/*
 * Copyright 2016 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.messaging.MessageChannel;

/**
 * A {@link Bindable} component that wraps a generic channel. Useful for binding channels outside the
 * {@link org.springframework.cloud.stream.annotation.Input} and {@link org.springframework.cloud.stream.annotation.Output}
 * annotated interfaces.
 *
 * @author Ilayaperumal Gopinathan
 */
public class SingleChannelBindable extends BindableAdapter {

	private final String name;

	private final MessageChannel messageChannel;

	public SingleChannelBindable(String name, MessageChannel messageChannel) {
		this.name = name;
		this.messageChannel = messageChannel;
	}

	@Override
	public void bindOutputs(ChannelBindingService adapter) {
		adapter.bindProducer(messageChannel, name);
	}

	@Override
	public void unbindOutputs(ChannelBindingService adapter) {
		adapter.unbindProducers(name);
	}

	@Override
	public Set<String> getOutputs() {
		return Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(name)));
	}
}
