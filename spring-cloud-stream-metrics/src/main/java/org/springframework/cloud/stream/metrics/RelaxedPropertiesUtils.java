/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.metrics;

import org.springframework.boot.bind.RelaxedNames;

/**
 * @author Vinicius Carvalho
 * Utiliy class to deal with {@link RelaxedNames}
 */
public class RelaxedPropertiesUtils {

	/**
	 * Searches relaxed names and try to find a canonical form (dot notation) of a property.
	 *
	 * For example, if a new list was built with JAVA_HOME as property, the return of this method would be java.home
	 *
	 * @param names - List of possible permutation of a property
	 * @return The canonical form (dot notation) of this property
	 */
	public static String findCanonicalFormat(RelaxedNames names){
		String canonicalForm = null;
		for(String name : names){
			if(name.contains(".") && upperCaseRatio(name) < 0.5){
				canonicalForm = name;
				break;
			}
			if(name.contains("-") && !name.contains("_")){
				canonicalForm = name.toLowerCase().replace("-",".");
				break;
			}
			if(name.contains("_") && !name.contains("-")){
				canonicalForm = name.toLowerCase().replace("_",".");
				break;
			}
		}
		//Safe guard for top level properties such as MEM, OS
		if(canonicalForm == null){
			canonicalForm = names.iterator().next().toLowerCase();
		}
		return canonicalForm;
	}

	/**
	 * Returns the ratio of uppercase chars on a string
	 * @param input
	 * @return
	 */
	private static double upperCaseRatio(String input){
		int upperCaseCount = 0;
		for(int i=0;i<input.length();i++){
			if(Character.isUpperCase(input.charAt(i))){
				upperCaseCount++;
			}
		}
		return (float)upperCaseCount/input.length();
	}


	public static void main(String[] args) {

		System.out.println(findCanonicalFormat(new RelaxedNames("OS")));
		System.out.println(findCanonicalFormat(new RelaxedNames("JAVA_HOME")));
		System.out.println(findCanonicalFormat(new RelaxedNames("spring.cloud.stream.metrics")));
		System.out.println(findCanonicalFormat(new RelaxedNames("SpringCloudStreamMetrics")));
		System.out.println(findCanonicalFormat(new RelaxedNames("SPRING_CLOUD_STREAM_METRICS")));
	}
}
