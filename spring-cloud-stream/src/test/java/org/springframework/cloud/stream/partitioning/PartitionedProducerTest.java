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

package org.springframework.cloud.stream.partitioning;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Marius Bogoevici
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(PartitionedProducerTest.TestSource.class)

public class PartitionedProducerTest {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Binder binder;

	@Autowired @Bindings(TestSource.class)
	private Source testSource;

	@Test
	@SuppressWarnings("unchecked")
	public void testBindingPartitionedProducer() {
		ArgumentCaptor<ProducerProperties> argumentCaptor = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("partOut"), eq(testSource.output()), argumentCaptor.capture());
		Assert.assertThat(argumentCaptor.getValue().getPartitionCount(), equalTo(3));
		Assert.assertThat(argumentCaptor.getValue().getPartitionKeyExpression().getExpressionString(),
				equalTo("payload"));
		verifyNoMoreInteractions(binder);
	}


	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	@Import(MockBinderRegistryConfiguration.class)
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/partitioned-producer-test.properties")
	public static class TestSource {

	}

}
