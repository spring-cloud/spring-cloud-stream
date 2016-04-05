/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka;

/**
 * @author Marius Bogoevici
 */
public class KafkaConsumerProperties {

	private int minPartitionCount = 1;

	private boolean autoCommitOffset = true;

	private boolean resetOffsets = false;

	private KafkaMessageChannelBinder.StartOffset startOffset = null;

	public void setMinPartitionCount(int minPartitionCount) {
		this.minPartitionCount = minPartitionCount;
	}

	public int getMinPartitionCount() {
		return minPartitionCount;
	}

	public boolean isAutoCommitOffset() {
		return autoCommitOffset;
	}

	public void setAutoCommitOffset(boolean autoCommitOffset) {
		this.autoCommitOffset = autoCommitOffset;
	}

	public boolean isResetOffsets() {
		return resetOffsets;
	}

	public void setResetOffsets(boolean resetOffsets) {
		this.resetOffsets = resetOffsets;
	}

	public KafkaMessageChannelBinder.StartOffset getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(KafkaMessageChannelBinder.StartOffset startOffset) {
		this.startOffset = startOffset;
	}
}
