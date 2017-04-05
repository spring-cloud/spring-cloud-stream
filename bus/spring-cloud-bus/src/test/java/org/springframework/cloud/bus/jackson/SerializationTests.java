/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.bus.jackson;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;
import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Dave Syer
 *
 */
public class SerializationTests {

	ObjectMapper mapper = new ObjectMapper();

	@Test
	public void vanillaDeserialize() throws Exception {
		this.mapper.registerModule(new SubtypeModule(RefreshRemoteApplicationEvent.class,
				EnvironmentChangeRemoteApplicationEvent.class));
		EnvironmentChangeRemoteApplicationEvent source = new EnvironmentChangeRemoteApplicationEvent(
				this, "foo", "bar", Collections.<String, String>emptyMap());
		String value = this.mapper.writeValueAsString(source);
		RemoteApplicationEvent event = this.mapper.readValue(value,
				RemoteApplicationEvent.class);
		assertTrue(event instanceof EnvironmentChangeRemoteApplicationEvent);
		assertNotNull(event.getId());
		assertTrue(event.getId().equals(source.getId()));
	}

	@Test
	public void deserializeOldValueWithNoId() throws Exception {
		this.mapper.registerModule(new SubtypeModule(RefreshRemoteApplicationEvent.class,
				EnvironmentChangeRemoteApplicationEvent.class));
		EnvironmentChangeRemoteApplicationEvent source = new EnvironmentChangeRemoteApplicationEvent(
				this, "foo", "bar", Collections.<String, String>emptyMap());
		String value = this.mapper.writeValueAsString(source);
		value = value.replaceAll(",\"id\":\"[a-f0-9-]*\"", "");
		RemoteApplicationEvent event = this.mapper.readValue(value,
				RemoteApplicationEvent.class);
		assertTrue(event instanceof EnvironmentChangeRemoteApplicationEvent);
		assertNotNull(event.getId());
		assertFalse(event.getId().equals(source.getId()));
	}

}
