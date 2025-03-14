/*
 * Copyright 2017-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

/**
 * @deprecated since 4.2.1 Please use org.springframework.cloud.stream.binder.kafka.common.BinderHeaderMapper
 *
 */
@Deprecated
public class BinderHeaderMapper extends org.springframework.cloud.stream.binder.kafka.common.BinderHeaderMapper {

	public BinderHeaderMapper(String... patterns) {
		super(patterns);
	}

	public BinderHeaderMapper() {
		super();
	}
}
