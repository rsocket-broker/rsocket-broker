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

package io.rsocket.broker.http.bridge.support;

import java.util.stream.Stream;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Copied in from Spring Cloud Commons and modified. Wrapper for {@link ObjectProvider}.
 *
 * @param <T> type of the object to fetch
 * @author Spencer Gibb
 * @author Olga Maciaszek-Sharma
 */
public class SimpleObjectProvider<T> implements ObjectProvider<T> {

	private final T object;

	public SimpleObjectProvider(T object) {
		this.object = object;
	}

	@Override
	public T getObject(Object... args) throws BeansException {
		return object;
	}

	@Override
	public T getIfAvailable() throws BeansException {
		return this.object;
	}

	@Override
	public T getIfUnique() throws BeansException {
		return object;
	}

	@Override
	public T getObject() throws BeansException {
		return object;
	}

	@Override
	public Stream<T> orderedStream() {
		return Stream.of(object);
	}
}
