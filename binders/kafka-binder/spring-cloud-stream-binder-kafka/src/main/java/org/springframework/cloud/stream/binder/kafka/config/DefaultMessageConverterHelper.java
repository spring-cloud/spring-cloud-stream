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

package org.springframework.cloud.stream.binder.kafka.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.cloud.function.context.config.MessageConverterHelper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Oleg Zhurakousky
 * @author Soby Chacko
 */
public class DefaultMessageConverterHelper implements MessageConverterHelper {

	@Override
	public boolean shouldFailIfCantConvert(Message<?> message) {
		return false;
	}

	public void postProcessBatchMessageOnFailure(Message<?> message, int index) {
		MessageHeaders headers = message.getHeaders();
		Set<String> headerKeySet = headers.keySet();
		List<String> matchingHeaderKeys = new ArrayList<>();

		for (String string : headerKeySet) {
			if (string.startsWith(KafkaHeaders.PREFIX)) {
				matchingHeaderKeys.add(string);
			}
		}
		for (String matchingHeaderKey : matchingHeaderKeys) {
			Object matchingHeaderValue = message.getHeaders().get(matchingHeaderKey);
			if (matchingHeaderValue instanceof ArrayList<?> list) {
				if (!list.isEmpty()) {
					list.remove(index);
				}
			}
		}
	}
}
