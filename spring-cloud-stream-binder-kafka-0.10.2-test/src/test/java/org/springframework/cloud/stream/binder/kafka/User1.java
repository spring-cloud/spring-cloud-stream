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

package org.springframework.cloud.stream.binder.kafka;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.specific.SpecificRecordBase;

import org.springframework.core.io.ClassPathResource;

/**
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 */
public class User1 extends SpecificRecordBase {

	@Nullable
	private String name;

	@Nullable
	private String favoriteColor;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFavoriteColor() {
		return this.favoriteColor;
	}

	public void setFavoriteColor(String favoriteColor) {
		this.favoriteColor = favoriteColor;
	}

	@Override
	public Schema getSchema() {
		try {
			return new Schema.Parser().parse(new ClassPathResource("schemas/users_v1.schema").getInputStream());
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Object get(int i) {
		if (i == 0) {
			return getName().toString();
		}
		if (i == 1) {
			return getFavoriteColor().toString();
		}
		return null;
	}

	@Override
	public void put(int i, Object o) {
		if (i == 0) {
			setName((String) o);
		}
		if (i == 1) {
			setFavoriteColor((String) o);
		}
	}
}
