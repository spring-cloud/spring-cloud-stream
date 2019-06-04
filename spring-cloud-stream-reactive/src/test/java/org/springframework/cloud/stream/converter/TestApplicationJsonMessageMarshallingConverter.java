package org.springframework.cloud.stream.converter;

import org.springframework.cloud.stream.reactive.MessageChannelToInputFluxParameterAdapterTests;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class exists because the {@link ApplicationJsonMessageMarshallingConverter} is package-private scoped, 
 * so it can't be used directly in, e.g. the {@link MessageChannelToInputFluxParameterAdapterTests} class.
 * In that case, we are just extending it and making it public (but only for tests).
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
