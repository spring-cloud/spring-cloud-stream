/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.schema;

import org.springframework.util.Assert;

/**
 * References a schema through its subject and version.
 *
 * @author Marius Bogoevici
 */
public class SchemaReference {

	private String subject;

	private int version;

	private String format;

	public SchemaReference(String subject, int version, String format) {
		Assert.hasText(subject, "cannot be empty");
		Assert.isTrue(version > 0, "must be a positive integer");
		Assert.hasText(format, "cannot be empty");
		this.subject = subject;
		this.version = version;
		this.format = format;
	}

	public String getSubject() {
		return this.subject;
	}

	public void setSubject(String subject) {
		Assert.hasText(subject, "cannot be empty");
		this.subject = subject;
	}

	public int getVersion() {
		return this.version;
	}

	public void setVersion(int version) {
		Assert.isTrue(version > 0, "must be a positive integer");
		this.version = version;
	}

	public String getFormat() {
		return this.format;
	}

	public void setFormat(String format) {
		Assert.hasText(format, "cannot be empty");
		this.format = format;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SchemaReference that = (SchemaReference) o;

		if (this.version != that.version) {
			return false;
		}
		if (!this.subject.equals(that.subject)) {
			return false;
		}
		return this.format.equals(that.format);

	}

	@Override
	public int hashCode() {
		int result = this.subject.hashCode();
		result = 31 * result + this.version;
		result = 31 * result + this.format.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "SchemaReference{" + "subject='" + this.subject + '\'' + ", version="
				+ this.version + ", format='" + this.format + '\'' + '}';
	}

}
