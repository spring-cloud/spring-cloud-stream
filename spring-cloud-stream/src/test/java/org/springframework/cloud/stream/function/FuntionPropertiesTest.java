/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.function;



import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Tolga Kavukcu
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = { FuntionPropertiesTest.MyFunctionsConfiguration.class })
public class FuntionPropertiesTest {

	@Autowired
	StreamFunctionProperties streamFunctionProperties;

	@Test
	public void converterCorrectlyInstalled() {
		assertThat(streamFunctionProperties.getProducerProperties().isUseNativeEncoding()).isEqualTo(Boolean.TRUE);
		assertThat(streamFunctionProperties.getConsumerProperties().getMaxAttempts()).isEqualTo(5);
	}

	@EnableAutoConfiguration
	@PropertySource("classpath:/org/springframework/cloud/stream/binder/cloud-function-test.properties")
	@Configuration
	public static class MyFunctionsConfiguration {

		@Bean
		public StreamFunctionProperties streamFunctionProperties() {
			return new StreamFunctionProperties();
		}
	}
}
