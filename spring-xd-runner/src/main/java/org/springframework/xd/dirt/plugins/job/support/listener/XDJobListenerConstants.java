/*
 * Copyright 2013-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.plugins.job.support.listener;

/**
 * Interface holdings constants related to XD JobListeners.
 * 
 * @author Ilayaperumal Gopinathan
 */
public interface XDJobListenerConstants {

	public static final String XD_JOB_EXECUTION_LISTENER_BEAN = "xd.jobExecutionListener";

	public static final String XD_STEP_EXECUTION_LISTENER_BEAN = "xd.stepExecutionListener";

	public static final String XD_CHUNK_LISTENER_BEAN = "xd.chunkListener";

	public static final String XD_ITEM_LISTENER_BEAN = "xd.itemListener";

	public static final String XD_SKIP_LISTENER_BEAN = "xd.skipListener";

	public static final String XD_JOB_EXECUTION_EVENTS_CHANNEL = "xd.job.jobExecutionEvents";

	public static final String XD_STEP_EXECUTION_EVENTS_CHANNEL = "xd.job.stepExecutionEvents";

	public static final String XD_CHUNK_EVENTS_CHANNEL = "xd.job.chunkEvents";

	public static final String XD_ITEM_EVENTS_CHANNEL = "xd.job.itemEvents";

	public static final String XD_SKIP_EVENTS_CHANNEL = "xd.job.skipEvents";

	public static final String XD_AGGREGATED_EVENTS_CHANNEL = "xd.job.aggregatedEvents";

	public static final String JOB_EXECUTION_EVENTS_SUFFIX = "job";

	public static final String STEP_EXECUTION_EVENTS_SUFFIX = "step";

	public static final String CHUNK_EVENTS_SUFFIX = "chunk";

	public static final String ITEM_EVENTS_SUFFIX = "item";

	public static final String SKIP_EVENTS_SUFFIX = "skip";

	public static final String[] XD_JOB_LISTENERS_SUFFIXES = new String[] { JOB_EXECUTION_EVENTS_SUFFIX,
		STEP_EXECUTION_EVENTS_SUFFIX, CHUNK_EVENTS_SUFFIX,
		ITEM_EVENTS_SUFFIX, SKIP_EVENTS_SUFFIX };

}
