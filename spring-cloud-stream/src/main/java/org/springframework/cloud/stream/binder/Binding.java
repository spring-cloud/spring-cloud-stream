/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.stream.binder;

/**
 * Represents a binding between an input or output and an adapter endpoint that connects
 * via a Binder. The binding could be for a consumer or a producer. A consumer binding
 * represents a connection from an adapter to an input. A producer binding represents a
 * connection from an output to an adapter.
 *
 * @author Jennifer Hickey
 * @author Mark Fisher
 * @author Gary Russell
 * @author Marius Bogoevici
 * @see org.springframework.cloud.stream.annotation.EnableBinding
 */
public interface Binding<T> {

	/**
	 * Unbinds the target component represented by this instance and stops any active
	 * components. Implementations must be idempotent. After this method is invoked, the
	 * target is not expected to receive any messages; this instance should be discarded,
	 * and a new Binding should be created instead.
	 */
	void unbind();
}
