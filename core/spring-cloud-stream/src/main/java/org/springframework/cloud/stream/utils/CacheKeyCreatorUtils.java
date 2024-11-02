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

package org.springframework.cloud.stream.utils;

import java.util.Objects;

import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;


/**
 * Utility methods related to Output Channel Cache Key.
 *
 * @author Omer Celik
 */
public final class CacheKeyCreatorUtils {

	private CacheKeyCreatorUtils() {
	}

	public static String createChannelCacheKey(@Nullable String binderName, String outputName,
			BindingServiceProperties bindingServiceProperties) {
		String finalBinderName = getBinderNameIfNeeded(binderName, outputName, bindingServiceProperties);
		return createChannelCacheKey(finalBinderName, outputName);
	}

	public static String getBinderNameIfNeeded(@Nullable String binderName, String outputName,
			BindingServiceProperties bindingServiceProperties) {
		if (Objects.isNull(binderName)) {
			return bindingServiceProperties.getBinder(outputName);
		}
		return binderName;
	}

	public static String createChannelCacheKey(String outputName, BindingServiceProperties bindingServiceProperties) {
		String binderName = bindingServiceProperties.getBinder(outputName);
		return createChannelCacheKey(binderName, outputName);
	}

	public static String createChannelCacheKey(String binderName, String outputName) {
		if (StringUtils.hasText(binderName)) {
			return binderName + ":" + outputName;
		}
		else {
			return outputName;
		}
	}
}
