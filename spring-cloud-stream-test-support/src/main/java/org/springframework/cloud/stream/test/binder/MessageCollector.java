/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.test.binder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

/**
 * Maintains a map between (output) channels and messages received (in FIFO order). To be injected in tests that
 * can then run assertions on the enqueued messages.
 *
 * @author Eric Bottard
 */
public class MessageCollector {

	private Map<MessageChannel, BlockingQueue<Message<?>>> results = new HashMap<>();

	/*package*/ BlockingQueue register(MessageChannel channel) {
		LinkedBlockingDeque<Message<?>> result = new LinkedBlockingDeque<>();
		Assert.isNull(results.put(channel, result));
		return result;
	}

	/*package*/ void unregister(MessageChannel channel) {
		Assert.notNull(results.remove(channel));
	}

	public BlockingQueue<Message<?>> forChannel(MessageChannel channel) {
		return results.get(channel);
	}
}
