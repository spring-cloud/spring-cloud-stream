/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.kafka.properties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Properties for configuring topics.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class KafkaAdminProperties {

	private Short replicationFactor;

	private Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();

	private Map<String, String> configuration = new HashMap<>();

	public Short getReplicationFactor() {
		return this.replicationFactor;
	}

	public void setReplicationFactor(Short replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public Map<Integer, List<Integer>> getReplicasAssignments() {
		return this.replicasAssignments;
	}

	public void setReplicasAssignments(Map<Integer, List<Integer>> replicasAssignments) {
		this.replicasAssignments = replicasAssignments;
	}

	public Map<String, String> getConfiguration() {
		return this.configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

}
