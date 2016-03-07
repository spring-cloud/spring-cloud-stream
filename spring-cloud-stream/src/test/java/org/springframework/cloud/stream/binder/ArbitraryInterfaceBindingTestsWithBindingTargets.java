/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Marius Bogoevici
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(ArbitraryInterfaceBindingTestsWithBindingTargets.TestFooChannels.class)
public class ArbitraryInterfaceBindingTestsWithBindingTargets {

	@Autowired
	@Bindings(ArbitraryInterfaceBindingTestsWithBindingTargets.TestFooChannels.class)
	public FooChannels fooChannels;

	@SuppressWarnings("rawtypes")
	@Autowired
	private Binder binder;

	@SuppressWarnings("unchecked")
	@Test
	public void testArbitraryInterfaceChannelsBound() {
		verify(binder).bindConsumer(eq("someQueue.0"), anyString(), eq(fooChannels.foo()), Mockito.<ConsumerProperties>any());
		verify(binder).bindConsumer(eq("someQueue.1"), anyString(), eq(fooChannels.bar()), Mockito.<ConsumerProperties>any());
		verify(binder).bindProducer(eq("someQueue.2"), eq(fooChannels.baz()), Mockito.<ProducerProperties>any());
		verify(binder).bindProducer(eq("someQueue.3"), eq(fooChannels.qux()), Mockito.<ProducerProperties>any());
		verifyNoMoreInteractions(binder);
	}

	@EnableBinding(FooChannels.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/arbitrary-binding-test.properties")
	public static class TestFooChannels {

	}

}
