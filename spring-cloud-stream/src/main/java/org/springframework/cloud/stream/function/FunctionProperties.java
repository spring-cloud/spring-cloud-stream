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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * @author Oleg Zhurakousky
 *
 * @since 2.1
 */
@ConfigurationProperties("spring.cloud.stream.function")
public class FunctionProperties {

	/**
	 * Name of functions to bind. If several functions need to be composed into one, use pipes (e.g., 'fooFunc|barFunc')
	 */
	private String name;


	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
