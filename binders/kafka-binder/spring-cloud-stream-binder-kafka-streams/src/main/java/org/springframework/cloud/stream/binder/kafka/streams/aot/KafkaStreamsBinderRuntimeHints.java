/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.aot;

import java.util.stream.Stream;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ProxyHints;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.cloud.stream.binder.kafka.streams.GlobalKTableBoundElementFactory;
import org.springframework.cloud.stream.binder.kafka.streams.KStreamBoundElementFactory;
import org.springframework.cloud.stream.binder.kafka.streams.KTableBoundElementFactory;
import org.springframework.lang.Nullable;


/**
 * {@link RuntimeHintsRegistrar} for the Kafka Streams binder in Spring Cloud Stream.
 *
 * @author Soby Chacko
 * @since 4.1.0
 */
public class KafkaStreamsBinderRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		// The following Kafka Streams specific (3rd party) hints will be removed
		// once these are added to https://github.com/oracle/graalvm-reachability-metadata
		registerKafkaStreamsReflectionHints(hints);
		registerKafkaStreamsJniHints(hints);
		hints.resources().registerPattern("*/kafka-streams-version.properties");

		// Binder specific hints
		ProxyHints proxyHints = hints.proxies();
		registerSpringJdkProxy(proxyHints, KStreamBoundElementFactory.KStreamWrapper.class, KStream.class);
		registerSpringJdkProxy(proxyHints, KTableBoundElementFactory.KTableWrapper.class, KTable.class);
		registerSpringJdkProxy(proxyHints, GlobalKTableBoundElementFactory.GlobalKTableWrapper.class, GlobalKTable.class);
	}

	private static void registerKafkaStreamsJniHints(RuntimeHints hints) {
		hints.jni().registerType(RocksDBException.class, MemberCategory.DECLARED_FIELDS, MemberCategory.INVOKE_DECLARED_METHODS);
		hints.jni().registerType(Status.class, MemberCategory.DECLARED_FIELDS, MemberCategory.INVOKE_DECLARED_METHODS);

		hints.resources().registerPattern("librocksdbjni-*");

	}

	private static void registerKafkaStreamsReflectionHints(RuntimeHints hints) {
		ReflectionHints reflectionHints = hints.reflection();

		Stream.of(
				org.apache.kafka.streams.errors.DefaultProductionExceptionHandler.class,
				org.apache.kafka.streams.errors.LogAndFailExceptionHandler.class,
				org.apache.kafka.streams.processor.FailOnInvalidTimestamp.class,
				org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier.class,
				org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor.class)
			.forEach(type -> reflectionHints.registerType(type,
				builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_METHODS)));

		reflectionHints.registerType(TypeReference.of("org.apache.kafka.streams.processor.internals.StateDirectory$StateDirectoryProcessFile"),
			MemberCategory.INVOKE_DECLARED_METHODS, MemberCategory.DECLARED_FIELDS, MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS);
	}

	private static void registerSpringJdkProxy(ProxyHints proxyHints, Class<?>... proxiedInterfaces) {
		proxyHints.registerJdkProxy(AopProxyUtils.completeJdkProxyInterfaces(proxiedInterfaces));
	}
}
