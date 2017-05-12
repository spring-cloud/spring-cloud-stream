/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.metrics.config;

import java.io.StringWriter;
import java.util.Date;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.json.JSONObject;
import org.junit.Test;

import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.cloud.stream.metrics.config.MetricJsonSerializer.Serializer;

import static org.junit.Assert.assertEquals;

/**
 * @author Oleg Zhurakousky
 */
public class MetricJsonSerializerTests {

	@Test
	public void validateAlwaysGMTDateAndFormat() throws Exception {
		Date date = new Date(1493060197188L); // Mon Apr 24 14:56:37 EDT 2017
		Metric<Number> metric = new Metric<Number>("Hello", 123, date);

		JsonFactory factory = new JsonFactory();
		StringWriter writer = new StringWriter();
		JsonGenerator jsonGenerator = factory.createGenerator(writer);
		Serializer ser = new Serializer();
		ser.serialize(metric, jsonGenerator, null);
		jsonGenerator.flush();

		JSONObject json = new JSONObject(writer.toString());
		String serializedTimestamp = json.getString("timestamp");
		assertEquals("2017-04-24T18:56:37.188Z", serializedTimestamp);
	}
}
