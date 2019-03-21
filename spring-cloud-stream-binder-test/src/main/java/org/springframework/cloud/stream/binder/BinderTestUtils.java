/*
 * Copyright 2014-2019 the original author or authors.
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

import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.integration.support.utils.IntegrationUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Gary Russell
 */
public abstract class BinderTestUtils {

	/**
	 * Mocked application context.
	 */
	public static final AbstractApplicationContext MOCK_AC = mock(
			AbstractApplicationContext.class);

	/**
	 * Mocked application bean factory.
	 */
	public static final ConfigurableListableBeanFactory MOCK_BF = mock(
			ConfigurableListableBeanFactory.class);

	private static final MessageBuilderFactory mbf = new MutableMessageBuilderFactory();

	static {
		when(MOCK_BF.getBean(
				IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				MessageBuilderFactory.class)).thenReturn(mbf);
		when(MOCK_AC.getBean(
				IntegrationUtils.INTEGRATION_MESSAGE_BUILDER_FACTORY_BEAN_NAME,
				MessageBuilderFactory.class)).thenReturn(mbf);
		when(MOCK_AC.getBeanFactory()).thenReturn(MOCK_BF);
	}

}
