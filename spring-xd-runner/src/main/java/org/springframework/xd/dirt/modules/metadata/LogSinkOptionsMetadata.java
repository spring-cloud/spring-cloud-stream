/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.modules.metadata;

import org.hibernate.validator.constraints.NotBlank;

import org.springframework.xd.module.options.spi.ModuleOption;
import org.springframework.xd.module.options.spi.ModulePlaceholders;


/**
 * Captures options for the {@code log} sink module.
 * 
 * @author Eric Bottard
 * @author Gary Russell
 */
public class LogSinkOptionsMetadata {

	private String name = ModulePlaceholders.XD_STREAM_NAME;

	private String expression = "payload";

	private String level = "INFO";

	@NotBlank
	public String getName() {
		return name;
	}

	@ModuleOption("the name of the log category to log to (will be prefixed by 'xd.sink.')")
	public void setName(String name) {
		this.name = name;
	}

	@NotBlank
	public String getExpression() {
		return expression;
	}

	@ModuleOption("the expression to be evaluated for the log content; use '#root' to log the full message")
	public void setExpression(String expression) {
		this.expression = expression;
	}

	@NotBlank
	public String getLevel() {
		return level;
	}

	@ModuleOption("the log level")
	public void setLevel(String level) {
		this.level = level.toUpperCase();
	}

}
