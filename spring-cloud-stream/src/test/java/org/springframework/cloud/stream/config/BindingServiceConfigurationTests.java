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

package org.springframework.cloud.stream.config;

import java.util.Map;

import org.junit.Test;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oleg Zhurakousky
 *
 */
public class BindingServiceConfigurationTests {

	@Test
	public void valdateImportedConfiguartionHandlerPostProcessing() {
		ApplicationContext context = new SpringApplicationBuilder(
				TestChannelBinderConfiguration
						.getCompleteConfiguration(RootConfiguration.class))
								.web(WebApplicationType.NONE).run();
		Map<String, AbstractReplyProducingMessageHandler> beansOfType = context
				.getBeansOfType(AbstractReplyProducingMessageHandler.class);
		for (AbstractReplyProducingMessageHandler handler : beansOfType.values()) {
			assertThat(handler.getNotPropagatedHeaders().contains("contentType"))
					.isTrue();
		}
	}

	@Configuration
	@Import(ImportedConfiguration.class)
	public static class RootConfiguration {

		@ServiceActivator(inputChannel = "input")
		public void rootService(String val) {
		}

	}

	@Configuration
	public static class ImportedConfiguration {

		@ServiceActivator(inputChannel = "input")
		public void importedService(String val) {
		}

	}

}
