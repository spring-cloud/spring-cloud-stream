/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.cloud.stream.config.codec;

import org.springframework.cloud.stream.config.codec.kryo.KryoCodecAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Generic {@link org.springframework.integration.codec.Codec} configuration imported by the
 * Binder configuration. This is the default configuration for 
 * <a href="https://github.com/EsotericSoftware/kryo">Kryo</a>. To provide an alternate Codec implementation, 
 * remove this configuration from the classpath and replace this class to resolve the Binder configuration 
 * reference.
 *
 * @author David Turanski
 */
@Configuration
@Import(KryoCodecAutoConfiguration.class)
public class CodecConfiguration {
}
