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

package org.springframework.cloud.stream.aggregate;

import org.springframework.beans.BeansException;
import org.springframework.cloud.stream.binding.BindingUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.Ordered;

/**
 * A listener that unbinds all {@link org.springframework.cloud.stream.binding.Bindable}s in a context
 * prior to shutdown. Used to coordinate child contexts in a Spring Boot parent-child hierarchy, where
 * child contexts are normally closed first, so this ensures that unbinding happens before that.
 *
 * @author Marius Bogoevici
 */
public class UnbindOnCloseApplicationListener implements ApplicationListener<ContextClosedEvent>, ApplicationContextAware, Ordered {

	private ConfigurableApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.applicationContext = (ConfigurableApplicationContext)applicationContext;
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

	@Override
	public void onApplicationEvent(ContextClosedEvent event) {
		BindingUtils.unbindAll(applicationContext);
	}
}
