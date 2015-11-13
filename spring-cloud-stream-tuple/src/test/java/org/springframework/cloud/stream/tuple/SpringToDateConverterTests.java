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

package org.springframework.cloud.stream.tuple;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class SpringToDateConverterTests {

	@Test
	public void testCtor() throws ParseException {
		StringToDateConverter converter = new StringToDateConverter();
		Date d = converter.convert("2013-05-02");

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		assertEquals(dateFormat.parse("2013-05-02"), d);
	}

}
