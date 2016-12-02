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
package org.springframework.cloud.stream.messaging;

import org.springframework.cloud.stream.annotation.Outputs;
import org.springframework.messaging.MessageChannel;

/**
 * Bindable interface with two output channels.
 *
 * @see org.springframework.cloud.stream.annotation.EnableBinding
 * @author Laabidi RAISSI
 */
public interface MultipleSources {

	String OUTPUT1 = "output1";
	String OUTPUT2 = "output2";

	@Outputs(value = MultipleSources.OUTPUT1+"," +MultipleSources.OUTPUT2)
	MessageChannel output(String name);
}
