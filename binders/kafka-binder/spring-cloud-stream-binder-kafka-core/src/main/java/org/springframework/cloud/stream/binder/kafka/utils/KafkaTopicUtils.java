/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.utils;

import java.nio.charset.StandardCharsets;

/**
 * Utility methods releated to Kafka topics.
 *
 * @author Soby Chacko
 */
public final class KafkaTopicUtils {

	private KafkaTopicUtils() {

	}

	/**
	 * Validate topic name. Allowed chars are ASCII alphanumerics, '.', '_' and '-'.
	 * @param topicName name of the topic
	 */
	public static void validateTopicName(String topicName) {
		byte[] utf8 = topicName.getBytes(StandardCharsets.UTF_8);
		for (byte b : utf8) {
			if (!((b >= 'a') && (b <= 'z') || (b >= 'A') && (b <= 'Z')
					|| (b >= '0') && (b <= '9') || (b == '.') || (b == '-')
					|| (b == '_'))) {
				throw new IllegalArgumentException(
						"Topic name can only have ASCII alphanumerics, '.', '_' and '-', but was: '"
								+ topicName + "'");
			}
		}
	}

}
