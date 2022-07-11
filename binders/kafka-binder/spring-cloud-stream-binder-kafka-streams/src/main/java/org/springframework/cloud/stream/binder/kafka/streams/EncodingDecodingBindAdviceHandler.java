/*
 * Copyright 2019-2022 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams;

import org.springframework.boot.context.properties.ConfigurationPropertiesBindHandlerAdvisor;
import org.springframework.boot.context.properties.bind.AbstractBindHandler;
import org.springframework.boot.context.properties.bind.BindContext;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;

/**
 * {@link ConfigurationPropertiesBindHandlerAdvisor} to detect nativeEncoding/Decoding settings
 * provided by the application explicitly.
 *
 * @author Soby Chacko
 * @since 3.0.0
 */
public class EncodingDecodingBindAdviceHandler implements ConfigurationPropertiesBindHandlerAdvisor {

	private boolean encodingSettingProvided;
	private boolean decodingSettingProvided;

	public boolean isDecodingSettingProvided() {
		return this.decodingSettingProvided;
	}

	public boolean isEncodingSettingProvided() {
		return this.encodingSettingProvided;
	}

	@Override
	public BindHandler apply(BindHandler bindHandler) {
		return new AbstractBindHandler(bindHandler) {
			@Override
			public <T> Bindable<T> onStart(ConfigurationPropertyName name,
										Bindable<T> target, BindContext context) {
				final String configName = name.toString();
				if (configName.contains("use") && configName.contains("native") &&
						(configName.contains("encoding") || configName.contains("decoding"))) {
					BindResult<T> result = context.getBinder().bind(name, target);
					if (result.isBound()) {
						if (configName.contains("encoding")) {
							EncodingDecodingBindAdviceHandler.this.encodingSettingProvided = true;
						}
						else {
							EncodingDecodingBindAdviceHandler.this.decodingSettingProvided = true;
						}
						return target.withExistingValue(result.get());
					}
				}
				return bindHandler.onStart(name, target, context);
			}
		};
	}
}
