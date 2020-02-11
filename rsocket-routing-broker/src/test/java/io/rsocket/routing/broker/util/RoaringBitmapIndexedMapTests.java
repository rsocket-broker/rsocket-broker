/*
 * Copyright 2020 the original author or authors.
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

package io.rsocket.routing.broker.util;

import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RoaringBitmapIndexedMapTests {

	@Test
	public void emptyTagsWorks() {
		RoaringBitmapIndexedMap<String> map = new RoaringBitmapIndexedMap<>();
		Id id = new Id(0, 1);
		map.put(id, id.toString(), Tags.builder().buildTags());
		Id id2 = new Id(0, 2);
		map.put(id2, id2.toString(), Tags.builder().buildTags());

		assertThat(map.get(id)).isEqualTo(id.toString());
		assertThat(map.get(id2)).isEqualTo(id2.toString());

		assertThat(map.values()).hasSize(2).contains(id.toString(), id2.toString());
		assertThat(map.query(Tags.builder().buildTags())).hasSize(2)
				.contains(id.toString(), id2.toString());

		assertThat(map.remove(id)).isEqualTo(id.toString());
		assertThat(map.size()).isEqualTo(1);

		assertThat(map.remove(id2)).isEqualTo(id2.toString());
		assertThat(map.isEmpty()).isTrue();
	}

	@Test
	public void indexedTagsWorks() {
		RoaringBitmapIndexedMap<String> map = new RoaringBitmapIndexedMap<>();
		Id id = new Id(0, 1);
		map.put(id, id.toString(), Tags.builder().with("tag1", "tag1value").buildTags());
		Id id2 = new Id(0, 2);
		map.put(id2, id2.toString(), Tags.builder().with("tag2", "tag2value").buildTags());

		assertThat(map.get(id)).isEqualTo(id.toString());
		assertThat(map.get(id2)).isEqualTo(id2.toString());

		assertThat(map.values()).hasSize(2).contains(id.toString(), id2.toString());
		assertThat(map.query(Tags.builder().with("tag1", "tag1value").buildTags())).hasSize(1)
				.contains(id.toString());
		assertThat(map.query(Tags.builder().with("tag2", "tag2value").buildTags())).hasSize(1)
				.contains(id2.toString());
	}
}
