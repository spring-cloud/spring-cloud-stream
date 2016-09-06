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

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.springframework.integration.kafka.support.ProducerMetadata;

/**
 * @author Marius Bogoevici
 */
public class KafkaProducerProperties {

	private int bufferSize = 16384;

	private int maxRequestSize = 1048576;

	private ProducerMetadata.CompressionType compressionType = ProducerMetadata.CompressionType.none;

	private boolean sync;

	private int batchTimeout;

	private Map<String, String> configuration = new HashMap<>();

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public int getMaxRequestSize() {
		return maxRequestSize;
	}

	public void setMaxRequestSize(int maxRequestSize) {
		this.maxRequestSize = maxRequestSize;
	}

	@NotNull
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

	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public Map<String, String> getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}
}
