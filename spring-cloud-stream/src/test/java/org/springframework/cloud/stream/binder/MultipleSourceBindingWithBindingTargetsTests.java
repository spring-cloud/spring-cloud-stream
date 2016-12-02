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
package org.springframework.cloud.stream.binder;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.MultipleSources;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Laabidi RAISSI
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MultipleSourceBindingWithBindingTargetsTests.TestSource.class)
public class MultipleSourceBindingWithBindingTargetsTests {

	
	@SuppressWarnings("rawtypes")
	@Autowired
	private BinderFactory binderFactory;

	@Autowired
	private MultipleSources testSource;

	@Autowired
	@Qualifier(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)
	private PublishSubscribeChannel errorChannel;

	@SuppressWarnings("unchecked")
	@Test
	public void testSourceOutputChannelBound() {
		Binder binder = binderFactory.getBinder(null);
		verify(binder).bindProducer(eq("testtock1"), eq(this.testSource.output("output1")),
				Mockito.<ProducerProperties>any());
		verify(binder).bindProducer(eq("testtock2"), eq(this.testSource.output("output2")),
				Mockito.<ProducerProperties>any());
		verifyNoMoreInteractions(binder);
	}

	@EnableBinding(MultipleSources.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/source-multiple-binding-test.properties")
	public static class TestSource {

	}
	
}
