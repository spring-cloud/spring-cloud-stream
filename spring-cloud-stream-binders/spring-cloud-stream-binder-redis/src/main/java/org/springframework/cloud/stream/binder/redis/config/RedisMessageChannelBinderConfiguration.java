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

package org.springframework.cloud.stream.binder.redis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.redis.RedisMessageChannelBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.integration.codec.Codec;

/**
 * @author David Turanski
 */
@Configuration
@EnableConfigurationProperties(RedisBinderConfigurationProperties.class)
@ConfigurationProperties(prefix = "spring.cloud.stream.binder.redis")
public class RedisMessageChannelBinderConfiguration {

	private String[] headers;

	@Autowired
	private Codec codec;

	@Autowired
	private RedisBinderConfigurationProperties redisBinderConfigurationProperties;

	@Autowired
	@Qualifier("binder")
	private RedisConnectionFactory redisConnectionFactory;

	@Bean
	public RedisMessageChannelBinder redisMessageChannelBinder() {

		RedisMessageChannelBinder redisMessageChannelBinder = new RedisMessageChannelBinder(this.redisConnectionFactory,
				this.headers);
		redisMessageChannelBinder.setCodec(this.codec);
		redisMessageChannelBinder.setDefaultBackOffInitialInterval(this.redisBinderConfigurationProperties.getBackOffInitialInterval());
		redisMessageChannelBinder.setDefaultBackOffMaxInterval(this.redisBinderConfigurationProperties.getBackOffMaxInterval());
		redisMessageChannelBinder.setDefaultBackOffMultiplier(this.redisBinderConfigurationProperties.getBackOffMultiplier());
		redisMessageChannelBinder.setDefaultConcurrency(this.redisBinderConfigurationProperties.getConcurrency());
		redisMessageChannelBinder.setDefaultMaxAttempts(this.redisBinderConfigurationProperties.getMaxAttempts());
		return redisMessageChannelBinder;
	}

	public String[] getHeaders() {
		return this.headers;
	}

	public void setHeaders(String[] headers) {
		this.headers = headers;
	}
}
