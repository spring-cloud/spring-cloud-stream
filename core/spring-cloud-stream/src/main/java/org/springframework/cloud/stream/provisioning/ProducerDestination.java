/*
 * Copyright 2015-present the original author or authors.
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

import org.springframework.cloud.stream.binder.ProducerProperties;

/**
 * Represents a ProducerDestination that provides the information about the destination
 * that is physically provisioned through
 * {@link ProvisioningProvider#provisionProducerDestination(String, ProducerProperties)}.
 *
 * @author Soby Chacko
 * @since 1.2
 */
public interface ProducerDestination {

	/**
	 * Provides the destination name.
	 * @return destination name
	 */
	String getName();

	/**
	 * Provides the destination name for a given partition.
	 *
	 * If the producer provision the destination with partitions, on certain middleware
	 * brokers there may exist multiple destinations distinguishable by the partition. For
	 * example, if the destination name is <b>xyz</b> and it is provisioned with <b>4</b>
	 * partitions, there may be 4 different destinations on the broker such as - <b>xyz-0,
	 * xyz-1, xyz-2 and xyz-3</b>. This behavior is dependent on the broker and the way
	 * the corresponding binder implements the logic.
	 *
	 * On certain brokers (for instance, Kafka), this behavior is completely skipped and
	 * there is a one-to-one correspondence between the destination name in the
	 * provisioner and the physical destination on the broker.
	 * @param partition the partition to find destination for
	 * @return destination name for the given partition
	 */
	String getNameForPartition(int partition);

}
