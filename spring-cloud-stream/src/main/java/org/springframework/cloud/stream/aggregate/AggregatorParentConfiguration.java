/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.cloud.stream.aggregate;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;

/**
 * Basic configuration for an aggregator application parent.
 *
 * @author Marius Bogoevici
 */
@EnableAutoConfiguration
@EnableBinding
public class AggregatorParentConfiguration {

	@Bean
	@ConditionalOnMissingBean(SharedChannelRegistry.class)
	public SharedChannelRegistry sharedChannelRegistry() {
		return new SharedChannelRegistry();
	}
}
