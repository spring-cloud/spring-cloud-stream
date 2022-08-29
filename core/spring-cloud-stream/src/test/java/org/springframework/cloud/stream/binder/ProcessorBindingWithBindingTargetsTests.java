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
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;

/**
 * @author Marius Bogoevici
 * @author Janne Valkealahti
 */
@ExtendWith(SpringExtension.class)
// @checkstyle:off
@SpringBootTest(classes = ProcessorBindingWithBindingTargetsTests.TestProcessor.class, properties = "spring.cloud.stream.defaultBinder=mock")
// @checkstyle:on
public class ProcessorBindingWithBindingTargetsTests {

	@Autowired
	private BinderFactory binderFactory;

	@Autowired
	private Processor testProcessor;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSourceOutputChannelBound() {
		final Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		verify(binder).bindConsumer(eq("testtock.0"), isNull(),
				eq(this.testProcessor.input()), Mockito.any());
		verify(binder).bindProducer(eq("testtock.1"), eq(this.testProcessor.output()),
				Mockito.any());
	}

	@EnableBinding(Processor.class)
	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/processor-binding-test.properties")
	public static class TestProcessor {

	}

}
