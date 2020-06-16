/*
 * Copyright 2019-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * A convenient {@link Serde} for {@link java.util.Collection} implementations.
 *
 * Whenever a Kafka Stream application needs to collect data into a container object like
 * {@link java.util.Collection}, then this Serde class can be used as a convenience for
 * serialization needs. Some examples of where using this may handy is when the application
 * needs to do aggregation or reduction operations where it needs to simply hold an
 * {@link Iterable} type.
 *
 * By default, this Serde will use {@link JsonSerde} for serializing the inner objects.
 * This can be changed by providing an explicit Serde during creation of this object.
 *
 * Here is an example of a possible use case:
 *
 * <pre class="code">
 *		.aggregate(ArrayList::new,
 * 					(k, v, aggregates) -&gt; {
 * 							aggregates.add(v);
 * 							return aggregates;
 *                                                },
 * 					Materialized.&lt;String, Collection&lt;Foo&gt;, WindowStore&lt;Bytes, byte[]&gt;&gt;as(
 * 						"foo-store")
 * 						.withKeySerde(Serdes.String())
 *						.withValueSerde(new CollectionSerde&lt;&gt;(Foo.class, ArrayList.class)))
 *  * </pre>
 *
 * Supported Collection types by this Serde are - {@link java.util.ArrayList}, {@link java.util.LinkedList},
 * {@link java.util.PriorityQueue} and {@link java.util.HashSet}. Deserializer will throw an exception
 * if any other Collection types are used.
 *
 * @param <E> type of the underlying object that the collection holds
 * @author Soby Chacko
 * @since 3.0.0
 */
public class CollectionSerde<E> implements Serde<Collection<E>> {

	/**
	 * Serde used for serializing the inner object.
	 */
	private final Serde<Collection<E>> inner;

	/**
	 * Type of the collection class. This has to be a class that is
	 * implementing the {@link java.util.Collection} interface.
	 */
	private final Class<?> collectionClass;

	/**
	 * Constructor to use when the application wants to specify the type
	 * of the Serde used for the inner object.
	 *
	 * @param serde specify an explicit Serde
	 * @param collectionsClass type of the Collection class
	 */
	public CollectionSerde(Serde<E> serde, Class<?> collectionsClass) {
		this.collectionClass = collectionsClass;
		this.inner =
				Serdes.serdeFrom(
						new CollectionSerializer<>(serde.serializer()),
						new CollectionDeserializer<>(serde.deserializer(), collectionsClass));
	}

	/**
	 * Constructor to delegate serialization operations for the inner objects
	 * to {@link JsonSerde}.
	 *
	 * @param targetTypeForJsonSerde target type used by the JsonSerde
	 * @param collectionsClass type of the Collection class
	 */
	public CollectionSerde(Class<?> targetTypeForJsonSerde, Class<?> collectionsClass) {
		this.collectionClass = collectionsClass;
		try (JsonSerde<E> jsonSerde = new JsonSerde(targetTypeForJsonSerde)) {

			this.inner = Serdes.serdeFrom(
					new CollectionSerializer<>(jsonSerde.serializer()),
					new CollectionDeserializer<>(jsonSerde.deserializer(), collectionsClass));
		}
	}

	@Override
	public Serializer<Collection<E>> serializer() {
		return inner.serializer();
	}

	@Override
	public Deserializer<Collection<E>> deserializer() {
		return inner.deserializer();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		inner.serializer().configure(configs, isKey);
		inner.deserializer().configure(configs, isKey);
	}

	@Override
	public void close() {
		inner.serializer().close();
		inner.deserializer().close();
	}

	private static class CollectionSerializer<E> implements Serializer<Collection<E>> {


		private Serializer<E> inner;

		CollectionSerializer(Serializer<E> inner) {
			this.inner = inner;
		}

		CollectionSerializer() { }

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {

		}

		@Override
		public byte[] serialize(String topic, Collection<E> collection) {
			final int size = collection.size();
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final DataOutputStream dos = new DataOutputStream(baos);
			final Iterator<E> iterator = collection.iterator();
			try {
				dos.writeInt(size);
				while (iterator.hasNext()) {
					final byte[] bytes = inner.serialize(topic, iterator.next());
					dos.writeInt(bytes.length);
					dos.write(bytes);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Unable to serialize the provided collection", e);
			}
			return baos.toByteArray();
		}

		@Override
		public void close() {
			inner.close();
		}
	}

	private static class CollectionDeserializer<E> implements Deserializer<Collection<E>> {
		private final Deserializer<E> valueDeserializer;
		private final Class<?> collectionClass;

		CollectionDeserializer(final Deserializer<E> valueDeserializer, Class<?> collectionClass) {
			this.valueDeserializer = valueDeserializer;
			this.collectionClass = collectionClass;
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public Collection<E> deserialize(String topic, byte[] bytes) {
			if (bytes == null || bytes.length == 0) {
				return null;
			}

			Collection<E> collection = getCollection();
			final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));

			try {
				final int records = dataInputStream.readInt();
				for (int i = 0; i < records; i++) {
					final byte[] valueBytes = new byte[dataInputStream.readInt()];
					final int read = dataInputStream.read(valueBytes);
					if (read != -1) {
						collection.add(valueDeserializer.deserialize(topic, valueBytes));
					}
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Unable to deserialize collection", e);
			}

			return collection;
		}

		@Override
		public void close() {
		}

		private Collection<E> getCollection() {
			Collection<E> collection;
			if (this.collectionClass.isAssignableFrom(ArrayList.class)) {
				collection = new ArrayList<>();
			}
			else if (this.collectionClass.isAssignableFrom(HashSet.class)) {
				collection = new HashSet<>();
			}
			else if (this.collectionClass.isAssignableFrom(LinkedList.class)) {
				collection = new LinkedList<>();
			}
			else if (this.collectionClass.isAssignableFrom(PriorityQueue.class)) {
				collection = new PriorityQueue<>();
			}
			else {
				throw new IllegalArgumentException("Unsupported collection type - " + this.collectionClass);
			}
			return collection;
		}
	}
}
