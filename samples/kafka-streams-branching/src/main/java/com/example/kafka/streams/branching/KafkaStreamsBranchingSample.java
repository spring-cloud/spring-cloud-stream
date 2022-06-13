/*
 * Copyright 2020-2022 the original author or authors.
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

package com.example.kafka.streams.branching;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(proxyBeanMethods = false)
class KafkaStreamsBranchingSample {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsBranchingSample.class, args);
	}

	public static class WordCountProcessorApplication {

		@Bean
		@SuppressWarnings("unchecked")
		public Function<KStream<Object, String>, KStream<?, WordCount>[]> process() {

			Predicate<Object, WordCount> isEnglish = (k, v) -> v.word.equals("english");
			Predicate<Object, WordCount> isFrench =  (k, v) -> v.word.equals("french");
			Predicate<Object, WordCount> isSpanish = (k, v) -> v.word.equals("spanish");

			return input -> input
					.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
					.groupBy((key, value) -> value)
					.windowedBy(TimeWindows.of(Duration.ofSeconds(6)))
					.count(Materialized.as("WordCounts-1"))
					.toStream()
					.map((key, value) -> new KeyValue<>(null,
							new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
					.split()
					.branch(isEnglish)
					.branch(isFrench)
					.branch(isSpanish)
					.noDefaultBranch()
					.values().toArray(new KStream[0]);
		}
	}

	static class WordCount {

		private String word;

		private long count;

		private Date start;

		private Date end;

		WordCount(String word, long count, Date start, Date end) {
			this.word = word;
			this.count = count;
			this.start = start;
			this.end = end;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public Date getStart() {
			return start;
		}

		public void setStart(Date start) {
			this.start = start;
		}

		public Date getEnd() {
			return end;
		}

		public void setEnd(Date end) {
			this.end = end;
		}
	}
}
