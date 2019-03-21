/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.provisioning;

import org.springframework.cloud.stream.binder.ConsumerProperties;

/**
 * Represents a ConsumerDestination that provides the information about the destination
 * that is physically provisioned through
 * {@link ProvisioningProvider#provisionConsumerDestination(String, String, ConsumerProperties)}.
 *
 * @author Soby Chacko
 * @since 1.2
 */
public interface ConsumerDestination {

	/**
	 * Provides the destination name.
	 * @return destination name
	 */
	String getName();

}
