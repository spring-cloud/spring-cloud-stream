/*
 * Copyright 2014-present the original author or authors.
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

/**
 * Strategy for determining the partition to which a message should be sent.
 *
 * @author Gary Russell
 */
public interface PartitionSelectorStrategy {

	/**
	 * Determine the partition based on a key. The partitionCount is 1 greater than the
	 * maximum value of a valid partition. Typical implementations will return
	 * {@code someValue % partitionCount}. The caller will apply that same modulo
	 * operation (as well as enforcing absolute value) if the value exceeds partitionCount
	 * - 1.
	 * @param key the key
	 * @param partitionCount the number of partitions
	 * @return the partition
	 */
	int selectPartition(Object key, int partitionCount);

}
