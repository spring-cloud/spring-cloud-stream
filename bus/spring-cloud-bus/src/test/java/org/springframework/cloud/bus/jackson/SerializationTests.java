/*
 * Copyright 2012-present the original author or authors.
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

package org.springframework.cloud.bus.jackson;

import java.util.Collections;

import org.junit.Test;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.cloud.bus.event.EnvironmentChangeRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RefreshRemoteApplicationEvent;
import org.springframework.cloud.bus.event.RemoteApplicationEvent;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dave Syer
 *
 */
public class SerializationTests {

	ObjectMapper mapper;

	@Test
	public void vanillaDeserialize() throws Exception {
		this.mapper = JsonMapper.builder()
			.addModule(new SubtypeModule(RefreshRemoteApplicationEvent.class,
					EnvironmentChangeRemoteApplicationEvent.class))
			.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, true)
			.build();
		EnvironmentChangeRemoteApplicationEvent source = new EnvironmentChangeRemoteApplicationEvent(this, "foo", "bar",
				Collections.<String, String>emptyMap());
		String value = this.mapper.writeValueAsString(source);
		RemoteApplicationEvent event = this.mapper.readValue(value, RemoteApplicationEvent.class);
		assertThat(event instanceof EnvironmentChangeRemoteApplicationEvent).isTrue();
		assertThat(event.getId()).isNotNull();
		assertThat(event.getId().equals(source.getId())).isTrue();
	}

	@Test
	public void deserializeOldValueWithNoId() throws Exception {
		this.mapper = JsonMapper.builder()
			.addModule(new SubtypeModule(RefreshRemoteApplicationEvent.class,
					EnvironmentChangeRemoteApplicationEvent.class))
			.build();
		EnvironmentChangeRemoteApplicationEvent source = new EnvironmentChangeRemoteApplicationEvent(this, "foo", "bar",
				Collections.<String, String>emptyMap());
		String value = this.mapper.writeValueAsString(source);
		value = value.replaceAll(",\"id\":\"[a-f0-9-]*\"", "");
		RemoteApplicationEvent event = this.mapper.readValue(value, RemoteApplicationEvent.class);
		assertThat(event instanceof EnvironmentChangeRemoteApplicationEvent).isTrue();
		assertThat(event.getId()).isNotNull();
		assertThat(event.getId().equals(source.getId())).isFalse();
	}

}
