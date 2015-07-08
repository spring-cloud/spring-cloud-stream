/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.streams.adapter;

import java.util.Collection;
import java.util.Collections;

import org.springframework.cloud.streams.config.MessageBusProperties;

/**
 * @author Dave Syer
 */
public class ChannelsMetadata {

	private Collection<OutputChannelBinding> outputChannels = Collections.emptySet();
	private Collection<InputChannelBinding> inputChannels = Collections.emptySet();
	private MessageBusProperties module;

	public MessageBusProperties getModule() {
		return this.module;
	}

	public void setModule(MessageBusProperties module) {
		this.module = module;
	}

	public Collection<OutputChannelBinding> getOutputChannels() {
		return this.outputChannels;
	}

	public void setOutputChannels(Collection<OutputChannelBinding> outputChannels) {
		this.outputChannels = outputChannels;
	}

	public Collection<InputChannelBinding> getInputChannels() {
		return this.inputChannels;
	}

	public void setInputChannels(Collection<InputChannelBinding> inputChannels) {
		this.inputChannels = inputChannels;
	}

}
