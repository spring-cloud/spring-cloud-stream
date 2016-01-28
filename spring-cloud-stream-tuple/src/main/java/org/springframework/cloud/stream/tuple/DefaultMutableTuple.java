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

package org.springframework.cloud.stream.tuple;

import java.util.List;

import org.springframework.core.convert.support.ConfigurableConversionService;

/**
 * Extension of {@link DefaultTuple} that also allows tuple {@link MutableTuple mutation}.
 *
 * <p>Note that this implementation is not threadsafe.</p>
 *
 * @author Eric Bottard
 */
public class DefaultMutableTuple extends DefaultTuple implements MutableTuple {

	/*package*/ DefaultMutableTuple(List<String> names, List<Object> values, ConfigurableConversionService configurableConversionService) {
		super(names, values, configurableConversionService);
	}

	@Override
	public void setValue(int index, Object value) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException();
		}
		values.set(index, value);
	}

	@Override
	public void setValue(String name, Object value) {
		int index = indexOf(name);
		if (index != -1) {
			setValue(index, value);
		} else {
			names.add(name);
			values.add(value);
		}
	}


}
