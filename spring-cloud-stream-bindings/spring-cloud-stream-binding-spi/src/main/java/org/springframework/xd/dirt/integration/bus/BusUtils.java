/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.integration.bus;

import java.util.regex.Pattern;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Message Bus utilities.
 *
 * @author Gary Russell
 */
public class BusUtils {

	/**
	 * The delimiter between a group and index when constructing a bus consumer/producer.
	 */
	public static final String GROUP_INDEX_DELIMITER = ".";

	/**
	 * The prefix for the consumer/producer when creating a tap.
	 */
	public static final String TAP_CHANNEL_PREFIX = "tap:";

	/**
	 * The prefix for the consumer/producer when creating a topic.
	 */
	public static final String TOPIC_CHANNEL_PREFIX = "topic:";

	public static final Pattern PUBSUB_NAMED_CHANNEL_PATTERN = Pattern.compile("[^.]+\\.(tap|topic):");

	public static String addGroupToPubSub(String group, String inputChannelName) {
		if (inputChannelName.startsWith(TAP_CHANNEL_PREFIX)
				|| inputChannelName.startsWith(TOPIC_CHANNEL_PREFIX)) {
			inputChannelName = group + "." + inputChannelName;
		}
		return inputChannelName;
	}

	public static String removeGroupFromPubSub(String name) {
		if (PUBSUB_NAMED_CHANNEL_PATTERN.matcher(name).find()) {
			return name.substring(name.indexOf(".") + 1);
		}
		else {
			return name;
		}
	}

	/**
	 * Determine whether the provided channel name represents a pub/sub channel (i.e. topic or tap).
	 * @param channelName name of the channel to check
	 * @return true if pub/sub.
	 */
	public static boolean isChannelPubSub(String channelName) {
		Assert.isTrue(StringUtils.hasText(channelName), "Channel name should not be empty/null.");
		// Check if the channelName starts with tap: or topic:
		return (channelName.startsWith(TAP_CHANNEL_PREFIX) || channelName.startsWith(TOPIC_CHANNEL_PREFIX));
	}

	/**
	 * Construct a pipe name from the group and index.
	 * @param group the group.
	 * @param index the index.
	 * @return the name.
	 */
	public static String constructPipeName(String group, int index) {
		return group + GROUP_INDEX_DELIMITER + index;
	}

	public static String constructTapPrefix(String group) {
		return TAP_CHANNEL_PREFIX + "stream:" + group;
	}

}
