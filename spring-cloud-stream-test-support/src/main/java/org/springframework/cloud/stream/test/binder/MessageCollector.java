/*
 * Copyright 2015-2019 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import java.util.concurrent.BlockingQueue;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * Maintains a map between (output) channels and messages received (in FIFO order). To be
 * injected in tests that can then run assertions on the enqueued messages.
 *
 * @author Eric Bottard
 */
public interface MessageCollector {

	/**
	 * Obtain a queue that will receive messages sent to the given channel.
	 * @param channel message channel
	 * @return blocking queue for stored message
	 */
	BlockingQueue<Message<?>> forChannel(MessageChannel channel);

}
