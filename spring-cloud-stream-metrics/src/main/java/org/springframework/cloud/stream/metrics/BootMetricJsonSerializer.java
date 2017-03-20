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

package org.springframework.cloud.stream.metrics;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.springframework.boot.actuate.metrics.Metric;
import org.springframework.boot.jackson.JsonComponent;

/**
 * @author Vinicius Carvalho
 */
@JsonComponent
public class BootMetricJsonSerializer {

	final static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	public static class Serializer extends JsonSerializer<Metric> {

		@Override
		public void serialize(Metric metric, JsonGenerator json, SerializerProvider serializerProvider)
				throws IOException {
			json.writeStartObject();
			json.writeStringField("name", metric.getName());
			json.writeNumberField("value", metric.getValue().doubleValue());
			json.writeStringField("timestamp", df.format(metric.getTimestamp()));
			json.writeEndObject();
		}
	}

	public static class Deserializer extends JsonDeserializer<Metric> {

		@Override
		public Metric deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			JsonNode node = p.getCodec().readTree(p);
			String name = node.get("name").asText();
			Number value = node.get("value").asDouble();
			Date timestamp = null;

			try {
				timestamp = df.parse(node.get("timestamp").asText());
			}
			catch (ParseException e) {
			}
			Metric<Number> metric = new Metric(name, value, timestamp);

			return metric;
		}

	}
}
