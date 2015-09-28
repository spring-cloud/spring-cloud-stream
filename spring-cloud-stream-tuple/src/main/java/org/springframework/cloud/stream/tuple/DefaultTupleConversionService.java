/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.cloud.stream.tuple;

import java.util.Collection;

import org.springframework.core.convert.ConversionService;
import org.springframework.format.support.DefaultFormattingConversionService;

/**
 * Base {@link ConversionService} implementation suitable for use with {@link Tuple}
 * 
 * @author David Turanski
 * 
 */
public class DefaultTupleConversionService extends DefaultFormattingConversionService {

	public DefaultTupleConversionService() {
		/*
		 * DefaultFormattingConversionService provides Collection -> Object conversion which will produce the first item
		 * if the target type matches. Here, this results in an unfortunate side effect, getTuple(List<Tuple> list)
		 * would return a Tuple. In this case it is preferable to treat it as an error if the argument is not a Tuple.
		 */
		removeConvertible(Collection.class, Object.class);
	}
}
