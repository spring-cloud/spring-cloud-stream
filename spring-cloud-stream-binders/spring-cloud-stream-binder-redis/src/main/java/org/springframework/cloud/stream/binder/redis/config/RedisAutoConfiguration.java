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
package org.springframework.cloud.stream.binder.redis.config;

/**
 * @author Mark Pollack
 */
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.redis.RedisProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;

/**
 * Creates a dedicated RedisConnectionFactory different from the one spring-boot autoconfigure
 * section that the bindings use in spring-cloud-stream.
 *
 * The configuration prefix is "spring.cloud.stream.module.redis" and contains the standard
 * properties to configure a redis connection, host, port, etc as well as the additional properties for
 * the sink, queue, key, etc.
 *
 * @author Eric Bottard
 */
@Configuration
@ConditionalOnClass({ JedisConnection.class, RedisOperations.class, Jedis.class })
@EnableConfigurationProperties
public class RedisAutoConfiguration extends org.springframework.boot.autoconfigure.redis.RedisAutoConfiguration {

    @Bean(name = "org.springframework.cloud.stream.binder.redis.RedisProperties")
    @ConfigurationProperties("spring.cloud.stream.binder.redis.default")
    @ConditionalOnMissingBean
    public RedisProperties redisSinkProperties() {
        return new RedisProperties();
    }


    @Configuration
    @ConditionalOnMissingClass("org.apache.commons.pool2.impl.GenericObjectPool")
    protected static class CustomRedisConnectionConfiguration extends RedisConnectionConfiguration {

        @Bean
        @Qualifier("binder")
        public JedisConnectionFactory binderRedisConnectionFactory()
                throws UnknownHostException {
            return super.redisConnectionFactory();
        }
    }

    @Configuration
    @ConditionalOnClass(GenericObjectPool.class)
    protected static class CustomRedisPooledConnectionConfiguration extends
            RedisPooledConnectionConfiguration {

        @Bean
        @Qualifier("binder")
        public JedisConnectionFactory binderRedisConnectionFactory()
                throws UnknownHostException {
            return super.redisConnectionFactory();
        }


    }
}
