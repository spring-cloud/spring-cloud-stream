/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Oleg Zhurakousky
 * @since 4.2
 */
public class StandardBatchUtils {
	
	public static String BATCH_HEADERS = "scst_batchHeaders";
	

	public static class BatchMessageBuilder {
		
		private final List<Object> payloads = new ArrayList<>();
		
		private final List<Map<String, Object>> batchHeaders = new ArrayList<>();
		
		private final Map<String, Object> headers = new HashMap<>();
		
		public BatchMessageBuilder addMessage(Object payload, Map<String, Object> batchHeaders) {
			this.payloads.add(payload);
			this.batchHeaders.add(batchHeaders);
			return this;
		}
		
		public BatchMessageBuilder addHeader(String key, Object value) {
			this.headers.put(key, value);
			return this;
		}
		
		public Message<List<Object>> build() {
			this.headers.put(BATCH_HEADERS, this.batchHeaders);
			return MessageBuilder.createMessage(payloads, new MessageHeaders(headers));
		}
	}
}
