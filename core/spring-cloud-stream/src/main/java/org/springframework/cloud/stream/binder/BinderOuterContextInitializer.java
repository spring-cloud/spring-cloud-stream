/*
 * Copyright 2022-2022 the original author or authors.
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

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

/**
 * An {@link ApplicationContextInitializer} that isets the outer application context as a bean under the name
 * 'outerContext' on a binder child context.
 *
 * @author Chris Bono
 */
public class BinderOuterContextInitializer implements ApplicationContextInitializer<GenericApplicationContext> {

	private ConfigurableApplicationContext outerContext;

	public ConfigurableApplicationContext getOuterContext() {
		return outerContext;
	}

	public void setOuterContext(ConfigurableApplicationContext outerContext) {
		this.outerContext = outerContext;
	}

	@Override
	public void initialize(GenericApplicationContext applicationContext) {
		// For backwards compatibility
		applicationContext.getBeanFactory().registerSingleton("outerContext", this.outerContext);
	}
}
