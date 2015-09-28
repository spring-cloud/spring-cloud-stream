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

package org.springframework.cloud.stream.tuple.kryo;

import java.util.ArrayList;
import java.util.List;

import org.springframework.integration.codec.kryo.AbstractKryoRegistrar;
import org.springframework.integration.codec.kryo.KryoRegistrar;
import org.springframework.cloud.stream.tuple.DefaultTuple;

import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

/**
 * A {@link KryoRegistrar}
 * used to register a Tuple serializer.
 * @author David Turanski
 * @since 1.2
 */
public class TupleKryoRegistrar extends AbstractKryoRegistrar {

	private final static int TUPLE_REGISTRATION_ID = 41;

	private final static int ARRAY_LIST_REGISTRATION_ID = 42;

	private final DefaultTupleSerializer defaultTupleSerializer = new DefaultTupleSerializer();

	private final CollectionSerializer collectionSerializer = new CollectionSerializer();


	@Override
	public List<Registration> getRegistrations() {
		List<Registration> registrations = new ArrayList<>(2);
		registrations.add(new Registration(DefaultTuple.class, defaultTupleSerializer, TUPLE_REGISTRATION_ID));
		registrations.add(new Registration(ArrayList.class, collectionSerializer, ARRAY_LIST_REGISTRATION_ID));
		return registrations;
	}
}
