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

package org.springframework.cloud.stream.schema.avro.v2;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;

/**
 * @author Marius Bogoevici
 */
public class User1 {

	@Nullable
	private String name;

	private int favoriteNumber;

	@Nullable
	private String favoriteColor;

	@AvroDefault("\"NYC\"")
	private String favoritePlace = "Boston";

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getFavoriteNumber() {
		return this.favoriteNumber;
	}

	public void setFavoriteNumber(int favoriteNumber) {
		this.favoriteNumber = favoriteNumber;
	}

	public String getFavoriteColor() {
		return this.favoriteColor;
	}

	public void setFavoriteColor(String favoriteColor) {
		this.favoriteColor = favoriteColor;
	}

	public String getFavoritePlace() {
		return this.favoritePlace;
	}

	public void setFavoritePlace(String favoritePlace) {
		this.favoritePlace = favoritePlace;
	}

}
