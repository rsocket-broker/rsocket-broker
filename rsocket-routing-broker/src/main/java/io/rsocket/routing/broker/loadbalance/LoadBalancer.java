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

package io.rsocket.routing.broker.loadbalance;

import java.util.List;
import java.util.StringJoiner;

import io.rsocket.RSocket;
import io.rsocket.routing.common.Tags;
import reactor.core.publisher.Mono;

public interface LoadBalancer {

	/**
	 * Choose the next server based on the load balancing algorithm.
	 * @param request - incoming request
	 * @return Mono for the response
	 */
	Mono<Response> choose(Request request);

	default Mono<Response> choose(List<RSocket> rSockets) {
		return choose(new Request(rSockets));
	}

	class Request {
		private final List<RSocket> rSockets;

		public Request(List<RSocket> rSockets) {
			this.rSockets = rSockets;
		}

		public List<RSocket> getRSockets() {
			return this.rSockets;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", Request.class.getSimpleName() + "[", "]")
					.add("rSockets=" + rSockets)
					.toString();
		}
	}

	class Response {

		private final RSocket rSocket;

		public Response(RSocket rSocket) {
			this.rSocket = rSocket;
		}

		public boolean hasRSocket() {
			return rSocket != null;
		}

		public RSocket getRSocket() {
			return this.rSocket;
		}

		/**
		 * Notification that the request completed.
		 * @param completionContext - completion context
		 */
		public void onComplete(CompletionContext completionContext) {}

	}

	class CompletionContext {

		private final Status status;

		private final Throwable throwable;

		public CompletionContext(Status status) {
			this(status, null);
		}

		public CompletionContext(Status status, Throwable throwable) {
			this.status = status;
			this.throwable = throwable;
		}

		public Status getStatus() {
			return this.status;
		}

		public Throwable getThrowable() {
			return this.throwable;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", CompletionContext.class
					.getSimpleName() + "[", "]")
					.add("status=" + status)
					.add("throwable=" + throwable)
					.toString();
		}

		/**
		 * Request status state.
		 */
		public enum Status {

			/** Request was handled successfully. */
			SUCCESSS,
			/** Request reached the server but failed due to timeout or internal error. */
			FAILED,
			/** Request did not go off box and should not be counted for statistics. */
			DISCARD,

		}

	}


	@FunctionalInterface
	interface Factory {

		LoadBalancer getInstance(Tags tags);

	}
}
