/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.config.codec.kryo;

import java.util.ArrayList;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.codec.Codec;
import org.springframework.integration.codec.kryo.FileKryoRegistrar;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.integration.codec.kryo.PojoCodec;


/**
 * Auto configures {@link PojoCodec} if Kryo is on the class path.
 * @author David Turanski
 */
@Configuration
@ConditionalOnClass(Kryo.class)
@EnableConfigurationProperties(KryoCodecProperties.class)
@ConditionalOnMissingBean(Codec.class)
public class KryoCodecAutoConfiguration {

	@Autowired
	ApplicationContext applicationContext;

	@Autowired
	KryoCodecProperties kryoCodecProperties;

	@Bean
	@ConditionalOnMissingBean(PojoCodec.class)
	public PojoCodec codec() {
		Map<String, KryoRegistrar> kryoRegistrarMap = applicationContext.getBeansOfType(KryoRegistrar
				.class);
		return new PojoCodec(new ArrayList<>(kryoRegistrarMap.values()), kryoCodecProperties.isReferences());
	}

	@Bean
	@ConditionalOnMissingBean(KryoRegistrar.class)
	public KryoRegistrar fileRegistrar() {
		return new FileKryoRegistrar();
	}
}
