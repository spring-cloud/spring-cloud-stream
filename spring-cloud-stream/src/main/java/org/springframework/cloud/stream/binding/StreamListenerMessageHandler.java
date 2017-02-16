/*
 * Copyright 2017 the original author or authors.
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

import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * @author Marius Bogoevici
 * @since 1.2
 */
public class StreamListenerMessageHandler extends AbstractReplyProducingMessageHandler {

	private final InvocableHandlerMethod invocableHandlerMethod;

	StreamListenerMessageHandler(InvocableHandlerMethod invocableHandlerMethod) {
		this.invocableHandlerMethod = invocableHandlerMethod;
	}

	@Override
	protected boolean shouldCopyRequestHeaders() {
		return false;
	}

	public boolean isVoid() {
		return invocableHandlerMethod.isVoid();
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		try {
			return this.invocableHandlerMethod.invoke(requestMessage);
		}
		catch (Exception e) {
			if (e instanceof MessagingException) {
				throw (MessagingException) e;
			}
			else {
				throw new MessagingException(requestMessage,
						"Exception thrown while invoking " + this.invocableHandlerMethod.getShortLogMessage(), e);
			}
		}
	}
}
