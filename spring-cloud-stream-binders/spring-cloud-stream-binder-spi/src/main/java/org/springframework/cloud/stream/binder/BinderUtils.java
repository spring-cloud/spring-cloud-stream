/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Binder utilities.
 *
 * @author Gary Russell
 */
public class BinderUtils {

	/**
	 * The delimiter between a group and index when constructing a binder consumer/producer.
	 */
	public static final String GROUP_INDEX_DELIMITER = ".";

	/**
	 * The prefix for the consumer/producer when creating a topic.
	 */
	public static final String TOPIC_CHANNEL_PREFIX = "topic:";

	/**
	 * Determine whether the provided channel name represents a pub/sub channel (i.e. topic or tap).
	 * @param channelName name of the channel to check
	 * @return true if pub/sub.
	 */
	public static boolean isChannelPubSub(String channelName) {
		Assert.isTrue(StringUtils.hasText(channelName), "Channel name should not be empty/null.");
		return channelName.startsWith(TOPIC_CHANNEL_PREFIX);
	}

	/**
	 * Construct a name comprised of the group and name.
	 * @param name the name.
	 * @param group the group.
	 * @return the constructed name.
	 */
	public static String groupedName(String name, String group) {
		return group == null ? name : group + BinderUtils.GROUP_INDEX_DELIMITER + name;
	}

}
