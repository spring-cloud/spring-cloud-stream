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

package org.springframework.cloud.stream.binder;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.EnableModule;
import org.springframework.cloud.stream.annotation.ModuleChannels;
import org.springframework.cloud.stream.annotation.Processor;
import org.springframework.cloud.stream.utils.MockBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Marius Bogoevici
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(ProcessorBindingTestsWithBindingTargets.TestProcessor.class)
public class ProcessorBindingTestsWithBindingTargets {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Binder binder;

	@Autowired @ModuleChannels(TestProcessor.class)
	private Processor testProcessor;

	@SuppressWarnings("unchecked")
	@Test
	public void testSourceOutputChannelBound() {
		verify(binder).bindConsumer(eq("testtock.0"), eq(testProcessor.input()), Mockito.<Properties>any());
		verify(binder).bindProducer(eq("testtock.1"), eq(testProcessor.output()), Mockito.<Properties>any());
	}

	@EnableModule(Processor.class)
	@EnableAutoConfiguration
	@Import(MockBinderConfiguration.class)
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/processor-binding-test.properties")
	public static class TestProcessor {

	}
}
