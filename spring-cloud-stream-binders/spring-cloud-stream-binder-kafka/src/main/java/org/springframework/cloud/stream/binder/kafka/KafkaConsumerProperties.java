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

	private boolean autoCommitOffset = true;

	private Boolean autoCommitOnError;

	private boolean resetOffsets;

	private KafkaMessageChannelBinder.StartOffset startOffset;

	private boolean enableDlq;

	private int recoveryInterval = 5000;

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

	public boolean isEnableDlq() {
		return enableDlq;
	}

	public void setEnableDlq(boolean enableDlq) {
		this.enableDlq = enableDlq;
	}

	public Boolean getAutoCommitOnError() {
		return autoCommitOnError;
	}

	public void setAutoCommitOnError(Boolean autoCommitOnError) {
		this.autoCommitOnError = autoCommitOnError;
	}

	public int getRecoveryInterval() {
		return recoveryInterval;
	}

	public void setRecoveryInterval(int recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}
}
