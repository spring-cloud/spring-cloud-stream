/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.test;

import org.springframework.messaging.Message;

/**
 * Implementation of binder endpoint that represents the source destination (e.g.,
 * destination from which messages will be received by Processor.INPUT). <br>
 * You can interact with it by calling {@link #send(Message)} operation.
 *
 * @author Oleg Zhurakousky
 *
 */
public class InputDestination extends AbstractDestination {

	/**
	 * Allows the {@link Message} to be sent to a Binder to be delegated to binder's input
	 * destination (e.g., Processor.INPUT).
	 * @param message message to send
	 */
	public void send(Message<byte[]> message) {
		this.getChannel().send(message);
	}

}
