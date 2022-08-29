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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@ExtendWith(SpringExtension.class)
// @checkstyle:off
@SpringBootTest(classes = SourceBindingWithBindingTargetsTests.TestSource.class, properties = "spring.cloud.stream.defaultBinder=mock")
// @checkstyle:on
public class SourceBindingWithBindingTargetsTests {

	@Autowired
	private BinderFactory binderFactory;

	@Autowired
	private Source testSource;

	@Autowired
	@Qualifier(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)
	private PublishSubscribeChannel errorChannel;

	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void testSourceOutputChannelBound() {
		Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		verify(binder).bindProducer(eq("testtock"), eq(this.testSource.output()),
				Mockito.any());
		verifyNoMoreInteractions(binder);
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/source-binding-test.properties")
	public static class TestSource {

	}

}
