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

package io.rsocket.routing.broker.rsocket;

import java.util.concurrent.CancellationException;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class ErrorOnDisconnectRSocket implements RSocket {

	private static final CancellationException CANCELLATION_EXCEPTION =
			new CancellationException("Connection has closed");
	private final RSocket delegate;
	private final MonoProcessor<Boolean> onCancelHook = MonoProcessor.create();

	public ErrorOnDisconnectRSocket(RSocket source) {
		this.delegate = source;
	}

	@Override
	public Mono<Void> onClose() {
		return delegate.onClose();
	}

	@Override
	public void dispose() {
		onCancelHook.onNext(true);
		onCancelHook.onComplete();
		delegate.dispose();
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		try {
			return wrapMono(delegate.requestResponse(payload));
		}
		catch (Throwable t) {
			payload.release();
			return Mono.error(t);
		}
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		try {
			return delegate.fireAndForget(payload);
		}
		catch (Throwable t) {
			payload.release();
			return Mono.error(t);
		}
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		try {
			return wrap(delegate.requestStream(payload));

		}
		catch (Throwable t) {
			payload.release();
			return Flux.error(t);
		}
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		try {
			return delegate.metadataPush(payload);
		}
		catch (Throwable t) {
			payload.release();
			return Mono.error(t);
		}
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		try {
			return wrap(delegate.requestChannel(payloads));
		}
		catch (Throwable t) {
			return Flux.error(t);
		}
	}

	private <T> Mono<T> wrapMono(Mono<T> source) {
		return Mono.from(wrap(Flux.from(source)));
	}

	private <T> Flux<T> wrap(Flux<T> source) {
		return Flux.from(new CancelSubscriptionPublisher<>(source));
	}

	private class CancelSubscriptionPublisher<T> implements Publisher<T> {
		private final Flux<T> delegate;

		private CancelSubscriptionPublisher(Flux<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			Disposable subscription =
					onCancelHook.subscribe(
							b -> {
								s.onError(CANCELLATION_EXCEPTION);
							});
			delegate.subscribe(wrapSubscriber(s, subscription));
		}

		private Subscriber<? super T> wrapSubscriber(
				Subscriber<? super T> s, Disposable cancelSubscription) {
			Subscriber<? super T> delegate = s;

			return new Subscriber<T>() {
				@Override
				public void onSubscribe(Subscription s) {
					delegate.onSubscribe(wrapSubscription(s, cancelSubscription));
				}

				@Override
				public void onNext(T t) {
					delegate.onNext(t);
				}

				@Override
				public void onError(Throwable t) {
					if (!cancelSubscription.isDisposed()) {
						cancelSubscription.dispose();
					}
					delegate.onError(t);
				}

				@Override
				public void onComplete() {
					if (!cancelSubscription.isDisposed()) {
						cancelSubscription.dispose();
					}
					delegate.onComplete();
				}
			};
		}

		private Subscription wrapSubscription(Subscription s, Disposable cancelSubscription) {
			return new Subscription() {
				@Override
				public void request(long n) {
					s.request(n);
				}

				@Override
				public void cancel() {
					if (!cancelSubscription.isDisposed()) {
						cancelSubscription.dispose();
					}
					s.cancel();
				}
			};
		}
	}
}
