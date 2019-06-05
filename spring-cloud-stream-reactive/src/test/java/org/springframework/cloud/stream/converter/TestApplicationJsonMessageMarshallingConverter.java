/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.converter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.cloud.stream.reactive.MessageChannelToInputFluxParameterAdapterTests;

/**
 * This class exists because the {@link ApplicationJsonMessageMarshallingConverter} is package-private scoped,
 * so it can't be used directly in, e.g. the {@link MessageChannelToInputFluxParameterAdapterTests} class.
 * In that case, we are just extending it and making it public (but only for tests).
 *
 * @author Ryan Dunckel
 */
public class TestApplicationJsonMessageMarshallingConverter
		extends ApplicationJsonMessageMarshallingConverter {

	public TestApplicationJsonMessageMarshallingConverter() {
		this(new ObjectMapper().configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false));
	}

	public TestApplicationJsonMessageMarshallingConverter(ObjectMapper objectMapper) {
		super(objectMapper);
	}

}
