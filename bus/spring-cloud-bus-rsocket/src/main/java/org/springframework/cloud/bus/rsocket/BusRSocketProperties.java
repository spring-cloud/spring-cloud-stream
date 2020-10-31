/*
 * Copyright 2015-2020 the original author or authors.
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

package org.springframework.cloud.bus.rsocket;

import java.util.LinkedHashMap;
import java.util.Map;

import io.rsocket.routing.common.MutableKey;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.style.ToStringCreator;
import org.springframework.util.CollectionUtils;

@ConfigurationProperties("spring.cloud.bus.rsocket")
public class BusRSocketProperties implements InitializingBean {

	private final Map<MutableKey, String> defaultTags = new LinkedHashMap<>();

	@Override
	public void afterPropertiesSet() {
		if (CollectionUtils.isEmpty(defaultTags)) {
			defaultTags.put(MutableKey.of("bus"), "true");
		}
	}

	public Map<MutableKey, String> getDefaultTags() {
		return this.defaultTags;
	}

	@Override
	public String toString() {
		return new ToStringCreator(this).append("defaultTags", defaultTags).toString();
	}

}
