/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.cloud.stream.binder.redis;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.integration.channel.DefaultHeaderChannelRegistry;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.utils.IntegrationUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.xd.dirt.integration.bus.serializer.MultiTypeCodec;


/**
 * Test support class for {@link RedisMessageChannelBinder}.
 *
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 */
public class RedisTestBinder extends AbstractTestBinder<RedisMessageChannelBinder> {

	private StringRedisTemplate template;

	public RedisTestBinder(RedisConnectionFactory connectionFactory) {
		template = new StringRedisTemplate(connectionFactory);
	}

	public RedisTestBinder(RedisConnectionFactory connectionFactory, MultiTypeCodec<Object> codec) {
		RedisMessageChannelBinder binder = new RedisMessageChannelBinder(connectionFactory, codec);
		GenericApplicationContext context = new GenericApplicationContext();
		context.getBeanFactory().registerSingleton(IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				new DefaultMessageBuilderFactory());
		DefaultHeaderChannelRegistry channelRegistry = new DefaultHeaderChannelRegistry();
		channelRegistry.setReaperDelay(Long.MAX_VALUE);
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.afterPropertiesSet();
		channelRegistry.setTaskScheduler(taskScheduler);
		context.getBeanFactory().registerSingleton(
				IntegrationContextUtils.INTEGRATION_HEADER_CHANNEL_REGISTRY_BEAN_NAME,
				channelRegistry);
		context.refresh();
		binder.setApplicationContext(context);
		setBinder(binder);
		template = new StringRedisTemplate(connectionFactory);
	}

	@Override
	public void cleanup() {
		if (!queues.isEmpty()) {
			for (String queue : queues) {
				template.delete(queue);
			}
		}
	}
}
