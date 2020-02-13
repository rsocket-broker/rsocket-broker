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

package io.rsocket.routing.broker;

import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.RouteJoin;
import io.rsocket.routing.frames.RouteSetup;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RoutingTableTests {

	@Test
	public void containsTagsWorks() {
		Tags tags = Tags.builder().with("key1", "key1value").with("key2", "key2value")
				.buildTags();
		RouteJoin routeJoin = RouteJoin.builder()
				.routeId(Id.random())
				.brokerId(Id.random())
				.serviceName("myservice")
				.with("key2", "key2value")
				.with("key1", "key1value")
				.with("key3", "key3value")
				.build();
		boolean test = RoutingTable.containsTags(tags).test(routeJoin);
		assertThat(test).isTrue();

		Tags tags2 = Tags.builder().with("key1", "key1value").with("key4", "key4value")
				.buildTags();
		boolean test2 = RoutingTable.containsTags(tags2).test(routeJoin);
		assertThat(test2).isFalse();

	}

}
