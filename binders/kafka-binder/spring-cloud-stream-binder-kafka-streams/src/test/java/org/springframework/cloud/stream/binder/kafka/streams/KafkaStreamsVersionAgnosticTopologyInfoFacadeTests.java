/*
 * Copyright 2022-2022 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link KafkaStreamsVersionAgnosticTopologyInfoFacade}.
 *
 * @author Chris Bono
 */
class KafkaStreamsVersionAgnosticTopologyInfoFacadeTests {

	private static final Properties STREAM_PROPS = new Properties();

	private static final StreamsBuilder STREAM_BUILDER = new StreamsBuilder();

	static {
		STREAM_PROPS.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-facade-tests-app");
		STREAM_PROPS.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		STREAM_BUILDER.<String, String>stream("foo").to("bar");
	}

	@Nested
	class KafkaStreams30 {

		@Test
		void sourceTopicsForStoreWithTopics() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams30.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams30("topic1"), "store1")).isTrue();
		}

		@Test
		void sourceTopicsForStoreWithNoTopics() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams30.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams30(), "store1")).isFalse();
		}

	}

	@Nested
	class KafkaStreams31_32 {

		@Test
		void sourceTopicsForStoreWithTopics() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams31_32.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams31_32("topic1"), "store1")).isTrue();
		}

		@Test
		void sourceTopicsForStoreWithNoTopics() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams31_32.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams31_32(), "store1")).isFalse();
		}

	}

	@Nested
	class KafkaStreams33 {

		@Test
		void sourceTopicsForStoreWithTopics() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams33.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams33("topic1"), "store1")).isTrue();
		}

		@Test
		void sourceTopicsForStoreWithNoTopics() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams33.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams33(), "store1")).isFalse();
		}
	}

	@Nested
	class NegativeCases {
		@Test
		void nullTopologyInfo() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreams30.class);
			TestToplogyInfoOneArg internalTopologyBuilder = null;
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreams30(internalTopologyBuilder), "store1")).isFalse();
		}

		@Test
		void sourceTopicsForStoreMethodNotFound() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreamsNoSourceTopicsMethod.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreamsNoSourceTopicsMethod(), "store1")).isFalse();
		}


		@Test
		void sourceTopicsForStoreThrowsException() {
			KafkaStreamsVersionAgnosticTopologyInfoFacade facade =
					new KafkaStreamsVersionAgnosticTopologyInfoFacade(TestKafkaStreamsThrowsError.class);
			assertThat(facade.streamsAppActuallyHasStore(new TestKafkaStreamsThrowsError(), "store1")).isFalse();
		}
	}

	/**
	 * A test version of KafkaStreams as it exists in kafka-streams 3.0 w/ a topology
	 * info field named 'internalTopologyBuilder' that in turn has a single-arg version
	 * of sourceTopicsForStore(String)' that holds the info we need.
	 */
	static class TestKafkaStreams30 extends KafkaStreams {

		private TestToplogyInfoOneArg internalTopologyBuilder;

		TestKafkaStreams30(String... topics) {
			this(new TestToplogyInfoOneArg(topics));
		}

		TestKafkaStreams30(TestToplogyInfoOneArg internalTopologyBuilder) {
			super(STREAM_BUILDER.build(), STREAM_PROPS);
			this.internalTopologyBuilder = internalTopologyBuilder;
		}
	}

	/**
	 * A test version of KafkaStreams as it exists in kafka-streams [3.1,3.2] w/ a topology
	 * info field named 'topologyMetadata' that in turn has a single-arg version
	 * of sourceTopicsForStore(String)' that holds the info we need.
	 */
	static class TestKafkaStreams31_32 extends KafkaStreams {

		private TestToplogyInfoOneArg topologyMetadata;

		TestKafkaStreams31_32(String... topics) {
			super(STREAM_BUILDER.build(), STREAM_PROPS);
			this.topologyMetadata = new TestToplogyInfoOneArg(topics);
		}
	}

	/**
	 * A test version of KafkaStreams as it exists in kafka-streams 3.3+ w/ a topology
	 * info field named 'topologyMetadata' that in turn has a two-arg version
	 * of sourceTopicsForStore(String,String)' that holds the info we need.
	 */
	static class TestKafkaStreams33 extends KafkaStreams {

		private TestToplogyInfoTwoArgs topologyMetadata;

		TestKafkaStreams33(String... topics) {
			super(STREAM_BUILDER.build(), STREAM_PROPS);
			this.topologyMetadata = new TestToplogyInfoTwoArgs(topics);
		}
	}

	static class TestToplogyInfoOneArg {
		private String[] sourceTopics;

		TestToplogyInfoOneArg(String... sourceTopics) {
			this.sourceTopics = sourceTopics;
		}

		public Collection<String> sourceTopicsForStore(String storeName) {
			return this.sourceTopics == null ? Collections.emptyList() : Arrays.asList(this.sourceTopics);
		}
	}

	static class TestToplogyInfoTwoArgs {
		private String[] sourceTopics;

		TestToplogyInfoTwoArgs(String... sourceTopics) {
			this.sourceTopics = sourceTopics;
		}

		public Collection<String> sourceTopicsForStore(String storeName, String topologyName) {
			return this.sourceTopics == null ? Collections.emptyList() : Arrays.asList(this.sourceTopics);
		}
	}

	/**
	 * A test version of KafkaStreams w/ a topology info field named 'internalTopologyBuilder'
	 * that in turn has NO 'sourceTopicsForStore' method to use.
	 */
	static class TestKafkaStreamsNoSourceTopicsMethod extends KafkaStreams {

		private TestTopologyInfoNoSourceTopicsMethod internalTopologyBuilder = new TestTopologyInfoNoSourceTopicsMethod();

		TestKafkaStreamsNoSourceTopicsMethod() {
			super(STREAM_BUILDER.build(), STREAM_PROPS);
		}

		static class TestTopologyInfoNoSourceTopicsMethod {

		}

	}

	/**
	 * A test version of KafkaStreams w/ a topology info field named 'internalTopologyBuilder'
	 * that in turn has a single-arg version of sourceTopicsForStore(String)' that throws
	 * an exception when invoked.
	 */
	static class TestKafkaStreamsThrowsError extends KafkaStreams {

		private TestToplogyInfoOneArg internalTopologyBuilder = new TestToplogyInfoOneArg() {
			@Override
			public Collection<String> sourceTopicsForStore(String storeName) {
				throw new RuntimeException("BOOM: " + storeName);
			}
		};

		TestKafkaStreamsThrowsError() {
			super(STREAM_BUILDER.build(), STREAM_PROPS);
		}

	}

}
