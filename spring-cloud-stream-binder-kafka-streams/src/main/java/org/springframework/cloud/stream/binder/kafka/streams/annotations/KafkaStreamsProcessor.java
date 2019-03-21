/*
 * Copyright 2017-2019 the original author or authors.
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

import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

/**
 * Bindable interface for {@link KStream} input and output.
 *
 * This interface can be used as a bindable interface with
 * {@link org.springframework.cloud.stream.annotation.EnableBinding} when both input and
 * output types are single KStream. In other scenarios where multiple types are required,
 * other similar bindable interfaces can be created and used. For example, there are cases
 * in which multiple KStreams are required on the outbound in the case of KStream
 * branching or multiple input types are required either in the form of multiple KStreams
 * and a combination of KStreams and KTables. In those cases, new bindable interfaces
 * compatible with the requirements must be created. Here are some examples.
 *
 * <pre class="code">
 *     interface KStreamBranchProcessor {
 *         &#064;Input("input")
 *         KStream&lt;?, ?&gt; input();
 *
 *         &#064;Output("output-1")
 *         KStream&lt;?, ?&gt; output1();
 *
 *         &#064;Output("output-2")
 *         KStream&lt;?, ?&gt; output2();
 *
 *         &#064;Output("output-3")
 *         KStream&lt;?, ?&gt; output3();
 *
 *         ......
 *
 *     }
 *</pre>
 *
 * <pre class="code">
 *     interface KStreamKtableProcessor {
 *         &#064;Input("input-1")
 *         KStream&lt;?, ?&gt; input1();
 *
 *         &#064;Input("input-2")
 *         KTable&lt;?, ?&gt; input2();
 *
 *         &#064;Output("output")
 *         KStream&lt;?, ?&gt; output();
 *
 *         ......
 *
 *     }
 *</pre>
 *
 * @author Marius Bogoevici
 * @author Soby Chacko
 */
public interface KafkaStreamsProcessor {

	/**
	 * Input binding.
	 * @return {@link Input} binding for {@link KStream} type.
	 */
	@Input("input")
	KStream<?, ?> input();

	/**
	 * Output binding.
	 * @return {@link Output} binding for {@link KStream} type.
	 */
	@Output("output")
	KStream<?, ?> output();

}
