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
package org.springframework.cloud.binder.apps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cloud.binder.apps.Station.Readings;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.util.MimeType;

public class Tester {

	public static void main(String[] args) {
		//String testData = "{"station":{"readings":[{"stationid":100,"customerid":200,"timestamp":"2017-07-15T08:09:10"}...
		MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
		Station station = new Station();

		List<Readings> readings = new ArrayList<>();
		Readings reading1 = new Readings();
		reading1.setCustomerid("12345");
		reading1.setStationid("fgh");
		readings.add(reading1);

		Readings reading2 = new Readings();
		reading2.setCustomerid("222");
		reading2.setStationid("hjk");
		readings.add(reading2);
		station.setReadings(readings);

		Message<?> message = converter.toMessage(station, new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE, MimeType.valueOf("application/json"))));
		System.out.println(new String((byte[])message.getPayload()));
		station =  (Station) converter.fromMessage(message, Station.class);
		System.out.println("Deserialized Station" + station);
	}

}
