/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.cloud.function.context.config.MessageConverterHelper;
import org.springframework.messaging.Message;

/**
 * @author Oleg Zhurakousky
 */
public class DefaultMessageConverterHelper implements MessageConverterHelper {

	@Override
	public boolean shouldFailIfCantConvert(Message<?> message) {
		return false;
	}

	public void postProcessBatchMessageOnFailure(Message<?> message, int index) {
		AtomicInteger deliveryAttempt = (AtomicInteger) message.getHeaders().get("deliveryAttempt");
		if (message.getHeaders().containsKey("amqp_batchedHeaders") && deliveryAttempt != null && deliveryAttempt.get() == 1) {
			ArrayList<?> list = (ArrayList<?>) message.getHeaders().get("amqp_batchedHeaders");
			list.remove(index);
		}
	}
}
