/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binding;

import org.junit.Test;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Artem Bilan
 * @since 1.3
 */
public class InvalidBindingConfigurationTests {

	@Test
	public void testDuplicateBeanByBindingConfig() {
		assertThatThrownBy(() -> SpringApplication.run(TestBindingConfig.class))
				.isInstanceOf(BeanDefinitionStoreException.class)
				.hasMessageContaining("bean definition with this name already exists")
				.hasMessageContaining(TestInvalidBinding.NAME).hasNoCause();
	}

	public interface TestInvalidBinding {

		String NAME = "testName";

		@Input(NAME)
		SubscribableChannel in();

		@Output(NAME)
		MessageChannel out();

	}

	@EnableBinding(TestInvalidBinding.class)
	@EnableAutoConfiguration
	public static class TestBindingConfig {

	}

}
