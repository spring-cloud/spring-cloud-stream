/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.cloud.stream.converter;

import org.springframework.util.MimeType;


/**
 * A {@link StringConvertingContentTypeResolver} that requires a the content-type to be present.
 * 
 * @author David Turanski
 */
// TODO: This will likely be pushed to core Spring
public class StrictContentTypeResolver extends StringConvertingContentTypeResolver {

	/**
	 * @param defaultMimeType the required {@link MimeType}
	 */
	public StrictContentTypeResolver(MimeType defaultMimeType) {
		super();
		setDefaultMimeType(defaultMimeType);
	}
}
