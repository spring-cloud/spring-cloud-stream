/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.streams.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.cloud.stream.binder.kafka.streams.properties.KafkaStreamsStateStoreProperties;

/**
 * Interface for Kafka Stream state store.
 *
 * This interface can be used to inject a state store specification into KStream building
 * process so that the desired store can be built by StreamBuilder and added to topology
 * for later use by processors. This is particularly useful when need to combine stream
 * DSL with low level processor APIs. In those cases, if a writable state store is desired
 * in processors, it needs to be created using this annotation. Here is the example.
 *
 * <pre class="code">
 *     &#064;StreamListener("input")
 *     &#064;KafkaStreamsStateStore(name="mystate", type= KafkaStreamsStateStoreProperties.StoreType.WINDOW,
 *     								size=300000)
 *	   public void process(KStream&lt;Object, Product&gt; input) {
 *         ......
 *     }
 * </pre>
 *
 * With that, you should be able to read/write this state store in your
 * processor/transformer code.
 *
 * <pre class="code">
 * 		new Processor&lt;Object, Product&gt;() {
 * 			WindowStore&lt;Object, String&gt; state;
 * 			&#064;Override
 *			public void init(ProcessorContext processorContext) {
 *			state = (WindowStore)processorContext.getStateStore("mystate");
 *				......
 *			}
 *		}
 * </pre>
 *
 * @author Lei Chen
 */

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)

public @interface KafkaStreamsStateStore {

	/**
	 * Provides name of the state store.
	 * @return name of state store.
	 */
	String name() default "";

	/**
	 * State store type.
	 * @return {@link KafkaStreamsStateStoreProperties.StoreType} of state store.
	 */
	KafkaStreamsStateStoreProperties.StoreType type() default KafkaStreamsStateStoreProperties.StoreType.KEYVALUE;

	/**
	 * Serde used for key.
	 * @return key serde of state store.
	 */
	String keySerde() default "org.apache.kafka.common.serialization.Serdes$StringSerde";

	/**
	 * Serde used for value.
	 * @return value serde of state store.
	 */
	String valueSerde() default "org.apache.kafka.common.serialization.Serdes$StringSerde";

	/**
	 * Length in milli-second of Windowed store window.
	 * @return length in milli-second of window(for windowed store).
	 */
	long lengthMs() default 0;

	/**
	 * Retention period for Windowed store windows.
	 * @return the maximum period of time in milli-second to keep each window in this
	 * store(for windowed store).
	 */
	long retentionMs() default 0;

	/**
	 * Whether catching is enabled or not.
	 * @return whether caching should be enabled on the created store.
	 */
	boolean cache() default false;

	/**
	 * Whether logging is enabled or not.
	 * @return whether logging should be enabled on the created store.
	 */
	boolean logging() default true;

}
