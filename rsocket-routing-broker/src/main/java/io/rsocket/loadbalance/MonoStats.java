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
package io.rsocket.loadbalance;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.rsocket.Payload;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

class MonoStats extends Mono<Payload> implements Subscription, Subscriber<Payload> {

	final Mono<Payload> upstream;
	final Stats stats;

	long startTime;
	Subscription                    s;
	CoreSubscriber<? super Payload> actual;

	volatile int state;
	static final AtomicIntegerFieldUpdater<MonoStats> STATE =
			AtomicIntegerFieldUpdater.newUpdater(MonoStats.class, "state");

	public MonoStats(Mono<Payload> upstream, Stats stats) {
		this.upstream = upstream;
		this.stats = stats;
	}

	@Override
	public void onSubscribe(Subscription s) {
		this.s = s;
		this.actual.onSubscribe(this);
	}

	@Override
	public void onNext(Payload payload) {
		this.actual.onNext(payload);
	}

	@Override
	public void onError(Throwable t) {
		if (this.state == 0 && STATE.compareAndSet(this, 0, 1)) {
			final Stats stats = this.stats;
			stats.stopRequest(this.startTime);
			stats.recordError(0.0);

			this.actual.onComplete();
		}
	}

	@Override
	public void onComplete() {
		if (this.state == 0 && STATE.compareAndSet(this, 0, 1)) {
			final Stats stats = this.stats;
			final long startTime = this.startTime;
			final long now = stats.stopRequest(startTime);
			stats.record(now - startTime);

			this.actual.onComplete();
		}
	}

	@Override
	public void request(long n) {
		this.s.request(n);
	}

	@Override
	public void cancel() {
		if (this.state == 0 && STATE.compareAndSet(this, 0, 2)) {
			this.s.cancel();

			this.stats.stopRequest(this.startTime);
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super Payload> subscriber) {
		this.startTime = this.stats.startRequest();
		this.actual = subscriber;
		this.upstream.subscribe(this);
	}
}
