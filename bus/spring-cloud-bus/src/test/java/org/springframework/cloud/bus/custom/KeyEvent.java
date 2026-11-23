/*
 * Copyright 2026-present the original author or authors.
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


package org.springframework.cloud.bus.custom;

import org.springframework.cloud.bus.event.RemoteApplicationEvent;

/**
 * Custom event with a public no-arg constructor next to a public constructor taking the
 * source, as application code usually writes them (GH-3162). It lives in its own package
 * so that it is not picked up by the tests that scan the packages of the bus.
 */
@SuppressWarnings("serial")
public class KeyEvent extends RemoteApplicationEvent {

	private String key;

	public KeyEvent() {
	}

	public KeyEvent(Object source, String originService, String destination, String key) {
		super(source, originService, () -> destination);
		this.key = key;
	}

	public String getKey() {
		return this.key;
	}

}

