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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Gary Russell
 * @author Oleg Zhurakousky
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = SourceBindingWithGlobalPropertiesTest.TestSource.class, properties = {
		"spring.cloud.stream.default.contentType=application/json",
		"spring.cloud.stream.bindings.output.destination=ticktock",
		"spring.cloud.stream.default.producer.requiredGroups=someGroup",
		"spring.cloud.stream.default.producer.partitionCount=1",
		"spring.cloud.stream.bindings.output.producer.headerMode=none",
		"spring.cloud.stream.bindings.output.producer.partitionCount=4",
		"spring.cloud.stream.defaultBinder=mock" })
public class SourceBindingWithGlobalPropertiesTest {

	@Autowired
	private BindingServiceProperties serviceProperties;

	@Test
	public void testGlobalPropertiesSet() {
		BindingProperties bindingProperties = this.serviceProperties
				.getBindingProperties(Source.OUTPUT);
		Assertions.assertThat(bindingProperties.getContentType())
				.isEqualTo("application/json");
		Assertions.assertThat(bindingProperties.getDestination()).isEqualTo("ticktock");
		Assertions.assertThat(bindingProperties.getProducer().getRequiredGroups())
				.containsExactly("someGroup"); // default propagates to producer
		Assertions.assertThat(bindingProperties.getProducer().getPartitionCount())
				.isEqualTo(4); // validates binding property takes precedence over default
		Assertions.assertThat(bindingProperties.getProducer().getHeaderMode())
				.isEqualTo(HeaderMode.none);
	}

	@EnableBinding(Source.class)
	@EnableAutoConfiguration
	public static class TestSource {

	}

}
