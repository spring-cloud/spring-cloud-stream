/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.cloud.stream.schema.avro;

import org.apache.avro.Schema;

/**
 * Provides function towards naming schema registry subjects for Avro files.
 *
 * @author David Kalosi
 */
public interface SubjectNamingStrategy {

	/**
	 * Takes the Avro schema on input and returns the generated subject under which the
	 * schema should be registered.
	 * @param schema schema to register
	 * @return subject name
	 */
	String toSubject(Schema schema);

}
