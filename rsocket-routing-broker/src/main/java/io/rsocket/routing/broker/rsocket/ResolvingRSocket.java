/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.routing.broker.rsocket;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

public final class ResolvingRSocket extends ResolvingOperator<RSocket>
    implements CoreSubscriber<RSocket>, RSocket {

  final Publisher<RSocket> rSocketMono;

  volatile Subscription s;

  static final AtomicReferenceFieldUpdater<ResolvingRSocket, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(ResolvingRSocket.class, Subscription.class, "s");

  public ResolvingRSocket(Publisher<RSocket> rSocketMono) {
    this.rSocketMono = rSocketMono;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onComplete() {
    final Subscription s = this.s;
    if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
      this.doFinally();
      return;
    }

    if (value == null) {
      this.terminate(new IllegalStateException("Source completed empty"));
    } else {
      this.complete(value);
    }
  }

  @Override
  public void onError(Throwable t) {
    final Subscription s = this.s;

    if (s == Operators.cancelledSubscription()
        || S.getAndSet(this, Operators.cancelledSubscription())
            == Operators.cancelledSubscription()) {
      this.doFinally();
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.doFinally();
    // terminate upstream which means retryBackoff has exhausted
    this.terminate(t);
  }

  @Override
  public void onNext(RSocket value) {
    if (this.s == Operators.cancelledSubscription()) {
      this.doOnValueExpired(value);
      return;
    }

    this.value = value;
    // volatile write and check on racing
    this.doFinally();
  }

  @Override
  protected void doSubscribe() {
    this.rSocketMono.subscribe(this);
  }

  @Override
  protected void doOnValueResolved(RSocket value) {
    value.onClose().subscribe(null, t -> this.invalidate(), this::invalidate);
  }

  @Override
  protected void doOnValueExpired(RSocket value) {
    value.dispose();
    this.dispose();
  }

  @Override
  protected void doOnDispose() {
    Operators.terminate(S, this);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new MonoInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new MonoInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new FluxInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new FluxInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new MonoInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  @Override
  public double availability() {
    final RSocket rSocket = this.valueIfResolved();
    return rSocket != null ? rSocket.availability() : 0.0;
  }

  static final class MonoInner<RESULT>
		  extends MonoDeferredResolution<RESULT, RSocket> {

    MonoInner(ResolvingRSocket parent, Payload payload, FrameType requestType) {
      super(parent, payload, requestType);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      if (rSocket != null) {
        Mono<?> source;
        switch (this.requestType) {
          case REQUEST_FNF:
            source = rSocket.fireAndForget(this.payload);
            break;
          case REQUEST_RESPONSE:
            source = rSocket.requestResponse(this.payload);
            break;
          case METADATA_PUSH:
            source = rSocket.metadataPush(this.payload);
            break;
          default:
            Operators.error(this.actual, new IllegalStateException("Should never happen"));
            return;
        }

        source.subscribe((CoreSubscriber) this);
      } else {
        parent.add(this);
      }
    }
  }

  static final class FluxInner<INPUT>
		  extends FluxDeferredResolution<INPUT, RSocket> {

    FluxInner(
        ResolvingRSocket parent, INPUT fluxOrPayload, FrameType requestType) {
      super(parent, fluxOrPayload, requestType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(RSocket rSocket, Throwable t) {
      if (isTerminated()) {
        return;
      }

      if (t != null) {
        if (this.requestType == FrameType.REQUEST_STREAM) {
          ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        }
        onError(t);
        return;
      }

      if (rSocket != null) {
        Flux<? extends Payload> source;
        switch (this.requestType) {
          case REQUEST_STREAM:
            source = rSocket.requestStream((Payload) this.fluxOrPayload);
            break;
          case REQUEST_CHANNEL:
            source = rSocket.requestChannel((Flux<Payload>) this.fluxOrPayload);
            break;
          default:
            Operators.error(this.actual, new IllegalStateException("Should never happen"));
            return;
        }

        source.subscribe(this);
      } else {
        parent.add(this);
      }
    }
  }
}
