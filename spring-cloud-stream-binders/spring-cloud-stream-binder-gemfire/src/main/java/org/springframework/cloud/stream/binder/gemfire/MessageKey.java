/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.binder.gemfire;

import java.io.Serializable;

import org.springframework.util.Assert;

/**
 * @author Patrick Peralta
 */
public class MessageKey  implements Serializable, Comparable<MessageKey> {
	private final long id;

	private final String producerId;

	public MessageKey(long id, String producerId) {
		Assert.notNull(producerId);
		this.id = id;
		this.producerId = producerId;
	}

	public long getId() {
		return id;
	}

	public String getProducerId() {
		return producerId;
	}

	@Override
	public int compareTo(MessageKey that) {
		int c = this.producerId.compareTo(that.producerId);
		return (c == 0 ? Long.compare(this.id, that.id) : c);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MessageKey that = (MessageKey) o;
		return id == that.id && producerId.equals(that.producerId);
	}

	@Override
	public int hashCode() {
		int result = (int) (id ^ (id >>> 32));
		result = 31 * result + producerId.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "MessageKey{" +
				"id=" + id +
				", memberId='" + producerId + '\'' +
				'}';
	}
}

