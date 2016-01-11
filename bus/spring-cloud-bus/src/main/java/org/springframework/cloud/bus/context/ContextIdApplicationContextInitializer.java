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

package org.springframework.cloud.bus.context;

import org.springframework.core.Ordered;

/**
 * Workaround for poor choice of ordering in the Spring Boot context id initializer. This
 * one prefers a locally provided application name, rather than one provided by the
 * platform. Since Spring Cloud apps send each other events on the Bus addressed by
 * context id, this can be important when the application name doesn't match the one
 * provided in local config.
 *
 * @author Dave Syer
 *
 */
public class ContextIdApplicationContextInitializer
		extends org.springframework.boot.context.ContextIdApplicationContextInitializer {

	/**
	 * Placeholder pattern to resolve for application name.
	 */
	private static final String NAME_PATTERN = "${spring.application.name:${spring.config.name:${vcap.application.name:application}}}";

	public ContextIdApplicationContextInitializer() {
		super(NAME_PATTERN);
		setOrder(Ordered.LOWEST_PRECEDENCE - 5);
	}

}
