/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.routing.http.bridge.core;

import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import org.junit.jupiter.api.Test;

import static io.rsocket.routing.http.bridge.core.TagBuilder.buildTags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link TagBuilder}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
class TagBuilderTests {

	@Test
	void shouldBuildTags() {
		String tagString = "key1=value1,key2=value2";

		Tags tags = buildTags(tagString);

		assertThat(tags.get("key1")).isEqualTo("value1");
		assertThat(tags.get("key2")).isEqualTo("value2");
	}

	@Test
	void shouldIgnoreNonPairTags() {
		String tagString = "key1=value1,key2=value2,valuex,key3=value3";

		Tags tags = buildTags(tagString);

		assertThat(tags.get("key1")).isEqualTo("value1");
		assertThat(tags.get("key2")).isEqualTo("value2");
		assertThat(tags.get("key3")).isEqualTo("value3");
		assertThat(tags.asMap().values()).doesNotContain("valuex");
	}

	@Test
	void shouldNotThrowExceptionOnEmptyTagString() {
		String tagString = "";

		assertThatCode(() -> buildTags(tagString))
				.doesNotThrowAnyException();
	}

	@Test
	void shouldNotThrowExceptionOnNullTagString() {
		String tagString = null;

		assertThatCode(() -> buildTags(tagString))
				.doesNotThrowAnyException();
	}

	@Test
	void shouldUseWellKnownKeysIfResolved(){
		String tagString = "INSTANCE_NAME=test-instance";

		Tags tags = buildTags(tagString);

		assertThat(tags.asMap()).hasSize(1);
		assertThat(tags.get(WellKnownKey.INSTANCE_NAME)).isEqualTo("test-instance");
	}
}
