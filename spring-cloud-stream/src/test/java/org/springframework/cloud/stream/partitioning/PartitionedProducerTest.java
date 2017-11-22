/*
 * Copyright 2015-2017 the original author or authors.
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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.utils.MockBinderRegistryConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PartitionedProducerTest.TestSource.class)
public class PartitionedProducerTest {

	@Autowired
	private BinderFactory binderFactory;

	@Autowired
	private Source testSource;

	@Test
	@SuppressWarnings("rawtypes")
	public void testBindingPartitionedProducer() {
		Binder binder = this.binderFactory.getBinder(null, MessageChannel.class);
		ArgumentCaptor<ProducerProperties> argumentCaptor = ArgumentCaptor.forClass(ProducerProperties.class);
		verify(binder).bindProducer(eq("partOut"), eq(this.testSource.output()), argumentCaptor.capture());
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
