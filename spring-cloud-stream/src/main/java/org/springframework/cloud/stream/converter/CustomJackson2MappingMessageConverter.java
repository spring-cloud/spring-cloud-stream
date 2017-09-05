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

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConversionException;

/**
 * Custom implementation of {@link org.springframework.messaging.converter.MappingJackson2MessageConverter} that handles special String cases.
 * If the target of conversion is a String, it tries to read it from a quoted json string, to properly remove the quotes, if it fails, it then just
 * returns the original string as it is just a raw json string needed for the target.
 *
 * @author Vinicius Carvalho
 */

public class CustomJackson2MappingMessageConverter extends MappingJackson2MessageConverter{

	public CustomJackson2MappingMessageConverter() {
		super();
		setSerializedPayloadClass(byte[].class);
		setStrictContentTypeMatch(true);
	}

	@Override
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
		try{
			return super.convertFromInternal(message, targetClass, conversionHint);
		}catch (MessageConversionException me){
			//Strings need special treatment
			if(targetClass.isAssignableFrom(String.class)){
				return message.getPayload();
			}
			throw me;
		}

	}

}
