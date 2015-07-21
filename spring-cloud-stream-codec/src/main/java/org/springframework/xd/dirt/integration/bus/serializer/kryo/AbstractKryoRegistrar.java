/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.xd.dirt.integration.bus.serializer.kryo;

import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.serializer.support.SerializationFailedException;

/**
 * @author David Turanski
 */
public abstract class AbstractKryoRegistrar implements KryoRegistrar {
	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	protected final static Kryo kryo = new Kryo();

	@Override
	public void registerTypes(Kryo kryo) {
		for (Registration registration : getRegistrations()) {
			register(kryo, registration);
		}
	}

	public abstract List<Registration> getRegistrations();

	protected void register(Kryo kryo, Registration registration) {
		int id = registration.getId();

		Registration existing = kryo.getRegistration(id);

		if (existing != null) {
			throw new SerializationFailedException(String.format("registration already exists %s", existing));
		}

		log.info("registering {} with serializer {}", registration, registration.getSerializer().getClass()
				.getName());

		kryo.register(registration);
	}
}
