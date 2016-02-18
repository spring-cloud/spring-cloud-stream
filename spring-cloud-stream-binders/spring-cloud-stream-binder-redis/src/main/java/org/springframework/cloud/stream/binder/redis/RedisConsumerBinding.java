/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.redis;

import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.DefaultBindingPropertiesAccessor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.integration.endpoint.AbstractEndpoint;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class RedisConsumerBinding extends DefaultBinding<MessageChannel> {

	private RedisOperations<String, String> redisOperations;

	private boolean durable;

	public RedisConsumerBinding(String name, String group, MessageChannel target, AbstractEndpoint endpoint, DefaultBindingPropertiesAccessor properties, RedisOperations<String, String> redisOperations, boolean durable) {
		super(name, group, target, endpoint, properties);
		Assert.notNull(redisOperations, "RedisOperations cannot be null");
		this.redisOperations = redisOperations;
		this.durable = durable;
	}

	@Override
	protected void afterUnbind() {
		if (!durable) {
			String key = RedisMessageChannelBinder.CONSUMER_GROUPS_KEY_PREFIX + getName();
			this.redisOperations.boundZSetOps(key).incrementScore(getGroup(), -1);
		}
	}
}
