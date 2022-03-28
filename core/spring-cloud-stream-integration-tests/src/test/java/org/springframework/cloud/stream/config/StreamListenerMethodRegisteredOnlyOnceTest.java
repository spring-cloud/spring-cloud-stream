/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.config;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.Mockito.verify;

/**
 * See issue https://github.com/spring-cloud/spring-cloud-stream/issues/1080
 *
 * StreamListener method called twice when using @SpyBean
 *
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class StreamListenerMethodRegisteredOnlyOnceTest {

	@Autowired
	private SomeSink sink;

	@SpyBean
	private SomeHandler handler;

	@Test
	public void should_handleSomeMessage() {
		this.sink.channel().send(new GenericMessage<>("Payload"));
		verify(this.handler).handleMessage(); // should only be invoked once.
	}

	public interface SomeSink {

		@Input(Sink.INPUT)
		SubscribableChannel channel();

	}

	@EnableBinding(SomeSink.class)
	@EnableAutoConfiguration
	public static class SomeHandler {

		@StreamListener(Sink.INPUT)
		public void handleMessage() {
		}

	}

}
