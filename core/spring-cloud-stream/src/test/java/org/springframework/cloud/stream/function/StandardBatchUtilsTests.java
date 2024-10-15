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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StandardBatchUtils.BatchMessageBuilder;
import org.springframework.messaging.Message;

/**
 * 
 */
public class StandardBatchUtilsTests {

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchMessageBuilder() {
		BatchMessageBuilder builder = new BatchMessageBuilder();
		builder.addMessage("foo", Collections.singletonMap("fooKey", "fooValue"));
		builder.addHeader("a", "a");
		builder.addMessage("bar", Collections.singletonMap("barKey", "barValue"));
		builder.addMessage("baz", Collections.singletonMap("bazKey", "bazValue"));
		
		Message<List<Object>> batchMessage = builder.build();
		
		List<Object> payloads = batchMessage.getPayload();
		assertThat(payloads.size()).isEqualTo(3);
		
		List<Map<String, Object>> batchHeaders = (List<Map<String, Object>>) batchMessage.getHeaders().get(StandardBatchUtils.BATCH_HEADERS);
		assertThat(batchHeaders.size()).isEqualTo(3);
		
		assertThat(payloads.get(0)).isEqualTo("foo");
		assertThat(batchHeaders.get(0).get("fooKey")).isEqualTo("fooValue");
		
		assertThat(payloads.get(1)).isEqualTo("bar");
		assertThat(batchHeaders.get(1).get("barKey")).isEqualTo("barValue");
		
		assertThat(batchMessage.getHeaders().get("a")).isEqualTo("a");
	}
}
