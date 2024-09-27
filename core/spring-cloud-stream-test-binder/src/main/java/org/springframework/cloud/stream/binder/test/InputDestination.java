/*
 * Copyright 2017-2021 the original author or authors.
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
	 * Allows the {@link Message} to be sent to a Binder to be delegated to a default binding
	 * destination (e.g., "function-in-0" for cases where you only have a single function with the name 'function').
	 * @param message message to send
	 */
	public void send(Message<?> message) {
		this.getChannel(0).send(message);
	}

	/**
	 * Allows the {@link Message} to be sent to a Binder's destination.<br>
	 * This needs a bit of clarification. Just like with any binder, 'destination'
	 * name and 'binding' name are usually the same unless additional configuration
	 * is provided. For example; Assume you have a function 'uppercase'. The
	 * 'binding' names for this function would be 'uppercase-in-0' (for input) and
	 * 'uppercase-out-0' (for output). The 'destination' names would match as well
	 * unless you decide to provide something like
	 * 'spring.cloud.stream.bindings.uppercase-in-0.destination=upper' at which
	 * point the binding names and destination names are different. <br>
	 * <br>
	 * So, it is important to remember that since this binder's goal is to emulate
	 * real binders and real messaging systems you are sending TO and receiving FROM
	 * destination (as if it was real broker destination) and binder will map and
	 * delegate what you send to the binder's destination to individual bindings as
	 * would the real binder.
	 *
	 * *
	 *
	 * <pre>
	 *
	 * // assume the following properties
	 * "--spring.cloud.function.definition=uppercase",
	 * "--spring.cloud.stream.bindings.uppercase-in-0.destination=upper",
	 * "--spring.cloud.stream.bindings.uppercase-out-0.destination=upperout"
	 *
	 * // send/receive
	 * inputDestination.send(message, "upper");
	 * Message&lt;byte[]&gt; resultMessage = outputDestination.receive(1000, "upperout");
	 *
	 * // if 'destination' property is not provided for both input and output
	 * inputDestination.send(message, "uppercase-in-0");
	 * Message&lt;byte[]&gt; resultMessage = outputDestination.receive(1000, "uppercase-out-0");
	 * </pre>
	 *
	 * @param message    message to send
	 * @param destinationName the name of the destination
	 */
	public void send(Message<?> message, String destinationName) {
		this.getChannelByName(destinationName).send(message);
	}

}
