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
package org.springframework.cloud.stream.config;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

/**
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration({ChannelBindingServiceTests.TestSink.class})
public class ChannelBindingServiceTests {

	@Autowired @Bindings(TestSink.class)
	private Sink testSink;

	@Autowired
	private ConfigurableApplicationContext applicationContext;

	public static final CountDownLatch latch = new CountDownLatch(1);

	@Test
	public void testApplicationContextStartup() throws Exception {
		Assert.isTrue(latch.getCount() == 1, "Application context shouldn't be started.");
	}

	@EnableBinding(Sink.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/config/channel/test-sink-channel-configurers.properties")
	public static class TestSink {

		@Autowired
		private ConfigurableApplicationContext applicationContext;

		@Bean
		public ContextRefreshedEventListener contextRefreshedEventListener(ConfigurableApplicationContext applicationContext) {
			return new ContextRefreshedEventListener(applicationContext);
		}
	}

	public static class ContextRefreshedEventListener implements ApplicationListener<ContextStartedEvent> {

		private ConfigurableApplicationContext applicationContext;

		public ContextRefreshedEventListener(ConfigurableApplicationContext applicationContext) {
			this.applicationContext = applicationContext;
		}

		@Override
		public void onApplicationEvent(ContextStartedEvent event) {
			ConfigurableApplicationContext source = (ConfigurableApplicationContext) event.getSource();
			if (source == this.applicationContext) {
				latch.countDown();
			}
		}
	}
}
