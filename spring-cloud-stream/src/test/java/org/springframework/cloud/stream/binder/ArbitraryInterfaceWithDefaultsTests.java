/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Janne Valkealahti
 */
// @checkstyle:off
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = ArbitraryInterfaceWithDefaultsTests.TestFooChannels.class, properties = "spring.cloud.stream.default-binder=mock")
public class ArbitraryInterfaceWithDefaultsTests {

	// @checkstyle:on

	@Autowired
	public FooChannels fooChannels;

	@Autowired
	private BinderFactory binderFactory;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testArbitraryInterfaceChannelsBound() {
		final Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		verify(binder).bindConsumer(eq("foo"), isNull(), eq(this.fooChannels.foo()),
				Mockito.any());
		verify(binder).bindConsumer(eq("bar"), isNull(), eq(this.fooChannels.bar()),
				Mockito.any());
		verify(binder).bindProducer(eq("baz"), eq(this.fooChannels.baz()), Mockito.any());
		verify(binder).bindProducer(eq("qux"), eq(this.fooChannels.qux()), Mockito.any());
		verifyNoMoreInteractions(binder);
	}

	@EnableBinding(FooChannels.class)
	@EnableAutoConfiguration
	public static class TestFooChannels {

	}

}
