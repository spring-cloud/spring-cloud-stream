/*
 * Copyright 2019-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Soby Chacko
 */
class CollectionSerdeTest {

	@Test
	public void testCollectionsSerde() {

		Foo foo1 = new Foo();
		foo1.setData("data-1");
		foo1.setNum(1);

		Foo foo2 = new Foo();
		foo2.setData("data-2");
		foo2.setNum(2);

		List<Foo> foos = new ArrayList<>();
		foos.add(foo1);
		foos.add(foo2);

		CollectionSerde<Foo> collectionSerde = new CollectionSerde<>(Foo.class, ArrayList.class);
		byte[] serialized = collectionSerde.serializer().serialize("", foos);

		Collection<Foo> deserialized = collectionSerde.deserializer().deserialize("", serialized);

		Iterator<Foo> iterator = deserialized.iterator();
		Foo foo1Retrieved  = iterator.next();
		assertThat(foo1Retrieved.getData()).isEqualTo("data-1");
		assertThat(foo1Retrieved.getNum()).isEqualTo(1);

		Foo foo2Retrieved  = iterator.next();
		assertThat(foo2Retrieved.getData()).isEqualTo("data-2");
		assertThat(foo2Retrieved.getNum()).isEqualTo(2);

	}

	static class Foo {

		private int num;
		private String data;

		Foo() {
		}

		public int getNum() {
			return num;
		}

		public void setNum(int num) {
			this.num = num;
		}

		public String getData() {
			return data;
		}

		public void setData(String data) {
			this.data = data;
		}
	}
}
