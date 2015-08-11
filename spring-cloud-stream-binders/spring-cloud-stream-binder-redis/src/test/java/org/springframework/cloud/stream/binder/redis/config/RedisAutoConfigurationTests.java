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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import static org.junit.Assert.assertNotNull;

/**
 * @author Mark Pollack
 */
public class RedisAutoConfigurationTests {

    private AnnotationConfigApplicationContext context;

    @Before
    public void setup() {
        this.context = new AnnotationConfigApplicationContext();
    }

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void testDefaultRedisConfiguration() throws Exception {
        load();
        assertNotNull(BeanFactoryAnnotationUtils.qualifiedBeanOfType(this.context.getBeanFactory(),
                JedisConnectionFactory.class, "binder"));
    }


    private void load(String... environment) {
        this.context = doLoad(environment);
    }

    private AnnotationConfigApplicationContext doLoad(String... environment) {
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(applicationContext, environment);
        applicationContext.register(RedisAutoConfiguration.class,
                PropertyPlaceholderAutoConfiguration.class);
        applicationContext.refresh();
        return applicationContext;
    }
}
