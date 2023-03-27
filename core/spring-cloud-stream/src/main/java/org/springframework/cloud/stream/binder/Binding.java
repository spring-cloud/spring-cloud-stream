/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder;

import java.util.Collections;
import java.util.Map;

import org.springframework.cloud.stream.endpoint.BindingsEndpoint;
import org.springframework.integration.core.Pausable;

/**
 * Represents a binding between an input or output and an adapter endpoint that connects
 * via a Binder. The binding could be for a consumer or a producer. A consumer binding
 * represents a connection from an adapter to an input. A producer binding represents a
 * connection from an output to an adapter.
 *
 * @param <T> type of a binding
 * @author Jennifer Hickey
 * @author Mark Fisher
 * @author Gary Russell
 * @author Marius Bogoevici
 * @author Oleg Zhurakousky
 * @see org.springframework.cloud.stream.annotation.EnableBinding
 */
public interface Binding<T> extends Pausable {

	default Map<String, Object> getExtendedInfo() {
		return Collections.emptyMap();
	}

	/**
	 * Stops the target component represented by this instance. NOTE: At the time the
	 * instance is created the component is already started. This operation is typically
	 * used by actuator to re-bind/re-start.
	 *
	 * @see BindingsEndpoint
	 */
	@Override
	default void start() {
	}

	/**
	 * Starts the target component represented by this instance. NOTE: At the time the
	 * instance is created the component is already started. This operation is typically
	 * used by actuator to re-bind/re-start.
	 *
	 * @see BindingsEndpoint
	 */
	@Override
	default void stop() {
	}

	/**
	 * Will always return false unless overriden.
	 */
	@Override
	default boolean isPaused() {
		return false;
	}

	/**
	 * Pauses the target component represented by this instance if and only if the
	 * component implements {@link Pausable} interface NOTE: At the time the instance is
	 * created the component is already started and active. This operation is typically
	 * used by actuator to pause/resume.
	 *
	 * @see BindingsEndpoint
	 */
	@Override
	default void pause() {
		this.stop();
	}

	/**
	 * Resumes the target component represented by this instance if and only if the
	 * component implements {@link Pausable} interface NOTE: At the time the instance is
	 * created the component is already started and active. This operation is typically
	 * used by actuator to pause/resume.
	 *
	 * @see BindingsEndpoint
	 */
	@Override
	default void resume() {
		this.start();
	}

	/**
	 * @return 'true' if the target component represented by this instance is running.
	 */
	@Override
	default boolean isRunning() {
		return false;
	}

	/**
	 * Returns the name of the destination for this binding.
	 * @return destination name
	 */
	default String getName() {
		return null;
	}

	/**
	 * Returns the name of the target for this binding (i.e., channel name).
	 * @return binding name
	 *
	 * @since 2.2
	 */
	default String getBindingName() {
		return null;
	}

	/**
	 * Returns the name of the binder for this binding.
	 * @return binder name
	 *
	 * @since 4.0.2
	 */
	default String getBinderName() {
		return null;
	}

	/**
	 * Returns the type of the binder for this binding.
	 * @return binder name
	 *
	 * @since 4.0.2
	 */
	default String getBinderType() {
		return null;
	}

	/**
	 * Unbinds the target component represented by this instance and stops any active
	 * components. Implementations must be idempotent. After this method is invoked, the
	 * target is not expected to receive any messages; this instance should be discarded,
	 * and a new Binding should be created instead.
	 */
	void unbind();

	/**
	 * Returns boolean flag representing this binding's type. If 'true' this binding is an
	 * 'input' binding otherwise it is 'output' (as in binding annotated with
	 * either @Input or @Output).
	 * @return 'true' if this binding represents an input binding.
	 */
	default boolean isInput() {
		throw new UnsupportedOperationException(
				"Binding implementation `" + this.getClass().getName()
						+ "` must implement this operation before it is called");
	}

}
