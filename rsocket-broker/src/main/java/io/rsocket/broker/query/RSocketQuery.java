/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.query;

import java.util.List;

import io.rsocket.RSocket;
import io.rsocket.broker.common.Tags;

/**
 * Represents a query object that allows users to find all RSocket instances that match a particular Tags query.
 */
@FunctionalInterface
public interface RSocketQuery {

	List<RSocket> query(Tags tags);
}
