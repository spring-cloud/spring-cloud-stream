/*
 * Copyright 2018-2020 the original author or authors.
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

package org.springframework.cloud.stream.function;

import java.util.Objects;

import org.springframework.util.MimeType;

/**
 * Simple key implementation combining a bindingName and a mime type to be used in a map.
 *
 * @author Gilles Iachelini
 */
final class BindingMimeTypeKey {

	private final String bindingName;
	private final MimeType mimeType;
	private final int hashCode;

	BindingMimeTypeKey(final String bindingName, final MimeType mimeType) {
		this.bindingName = bindingName;
		this.mimeType = mimeType;
		this.hashCode = Objects.hash(bindingName, mimeType);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof BindingMimeTypeKey)) {
			return false;
		}
		BindingMimeTypeKey other = (BindingMimeTypeKey) obj;
		return Objects.equals(bindingName, other.bindingName)
				&& Objects.equals(mimeType, other.mimeType);
	}

	@Override
	public String toString() {
		return String.format("Bindingname: %s, Mimetype: %s", bindingName, mimeType);
	}
}
