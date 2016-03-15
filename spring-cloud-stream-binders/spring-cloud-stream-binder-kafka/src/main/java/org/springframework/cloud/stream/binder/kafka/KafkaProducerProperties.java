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

import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.integration.kafka.support.ProducerMetadata;

/**
 * @author Marius Bogoevici
 */
public class KafkaProducerProperties extends ProducerProperties {

	private int bufferSize = 16384;

	private ProducerMetadata.CompressionType compressionType = ProducerMetadata.CompressionType.none;

	private boolean sync = false;

	private KafkaMessageChannelBinder.Mode mode = KafkaMessageChannelBinder.Mode.embeddedHeaders;

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public ProducerMetadata.CompressionType getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(ProducerMetadata.CompressionType compressionType) {
		this.compressionType = compressionType;
	}

	public boolean isSync() {
		return sync;
	}

	public void setSync(boolean sync) {
		this.sync = sync;
	}

	public KafkaMessageChannelBinder.Mode getMode() {
		return mode;
	}

	public void setMode(KafkaMessageChannelBinder.Mode mode) {
		this.mode = mode;
	}

}
