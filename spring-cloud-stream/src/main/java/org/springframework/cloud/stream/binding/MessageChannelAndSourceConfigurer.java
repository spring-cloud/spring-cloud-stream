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

package org.springframework.cloud.stream.binding;

import org.springframework.cloud.stream.binder.PollableMessageSource;

/**
 * Configurer for {@link PollableMessageSource}.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public interface MessageChannelAndSourceConfigurer extends MessageChannelConfigurer {

	/**
	 * Configure the provided message source binding.
	 * @param binding the binding.
	 * @param name the name.
	 */
	void configurePolledMessageSource(PollableMessageSource binding, String name);

}
