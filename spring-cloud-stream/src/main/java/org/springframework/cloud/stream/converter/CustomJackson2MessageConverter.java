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

package org.springframework.cloud.stream.converter;

import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * Custom implementation of {@link MappingJackson2MessageConverter} that is more flexible on accepting json contentTypes.
 * Instead of strict matching for application/json this implemntation will accept any contentType that contains the keyword json on it.
 * This will allow custom types such as application/vnd.spring.cloud.stream.metric.v1+json to be supported for example.
 *
 * @author Vinicius Carvalho
 */
public class CustomJackson2MessageConverter extends MappingJackson2MessageConverter {

	@Override
	protected boolean supportsMimeType(MessageHeaders headers) {
		if (getSupportedMimeTypes().isEmpty()) {
			return true;
		}
		MimeType mimeType = getMimeType(headers);
		if (mimeType == null) {
			return !isStrictContentTypeMatch();
		}
		return mimeType.getSubtype().toLowerCase().contains(MimeTypeUtils.APPLICATION_JSON.getSubtype());

	}

}
