/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kinesis.properties;

/**
 * The Kinesis-specific producer binding configuration properties.
 *
 * @author Peter Oates
 * @author Jacob Severson
 *
 */
public class KinesisProducerProperties {

	private boolean sync;

	private long sendTimeout = 10000;

	public void setSync(boolean sync) {
		this.sync = sync;
	}

	public boolean isSync() {
		return this.sync;
	}

	public long getSendTimeout() {
		return this.sendTimeout;
	}

	public void setSendTimeout(long sendTimeout) {
		this.sendTimeout = sendTimeout;
	}

}
