/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.cloud.stream.schema.registry.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

/**
 * @author Vinicius Carvalho
 *
 * Represents a persisted schema entity.
 */
@Entity
@Table(name = "SCHEMA_REPOSITORY")
public class Schema {

	@Id
	@GeneratedValue
	@Column(name = "ID")
	private Integer id;

	@Column(name = "VERSION", nullable = false)
	private Integer version;

	@Column(name = "SUBJECT", nullable = false)
	private String subject;

	@Column(name = "FORMAT", nullable = false)
	private String format;

	@Lob
	@Column(name = "DEFINITION", nullable = false, length = 8192)
	private String definition;

	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getVersion() {
		return this.version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public String getSubject() {
		return this.subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getFormat() {
		return this.format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getDefinition() {
		return this.definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

}
