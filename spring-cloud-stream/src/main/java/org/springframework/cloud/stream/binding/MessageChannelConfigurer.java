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
package org.springframework.cloud.stream.binding;

import org.springframework.messaging.MessageChannel;

/**
 * Interface to be implemented by the classes that configure the {@link Bindable} message channels.
 *
 * @author Ilayaperumal Gopinathan
 */
public interface MessageChannelConfigurer {

	/**
	 * Configure the given message channel.
	 *
	 * @param messageChannel the message channel
	 * @param channelName name of the message channel
	 */
	void configureMessageChannel(MessageChannel messageChannel, String channelName);
}
