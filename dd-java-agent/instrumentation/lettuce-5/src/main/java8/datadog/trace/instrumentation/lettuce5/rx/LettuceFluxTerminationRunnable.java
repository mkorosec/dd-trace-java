package datadog.trace.instrumentation.lettuce5.rx;

import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.startSpan;
import static datadog.trace.instrumentation.lettuce5.LettuceClientDecorator.DECORATE;
import static datadog.trace.instrumentation.lettuce5.LettuceClientDecorator.REDIS_QUERY;

import datadog.trace.bootstrap.instrumentation.api.AgentSpan;
import io.lettuce.core.protocol.RedisCommand;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.publisher.SignalType;

public class LettuceFluxTerminationRunnable implements Consumer<Signal>, Runnable {

  private AgentSpan span = null;
  private int numResults = 0;
  private FluxOnSubscribeConsumer onSubscribeConsumer = null;

  public LettuceFluxTerminationRunnable(
      final RedisCommand command, final boolean finishSpanOnClose) {
    onSubscribeConsumer = new FluxOnSubscribeConsumer(this, command, finishSpanOnClose);
  }

  public FluxOnSubscribeConsumer getOnSubscribeConsumer() {
    return onSubscribeConsumer;
  }

  private void finishSpan(final boolean isCommandCancelled, final Throwable throwable) {
    if (span != null) {
      span.setTag("db.command.results.count", numResults);
      if (isCommandCancelled) {
        span.setTag("db.command.cancelled", true);
      }
      DECORATE.onError(span, throwable);
      DECORATE.beforeFinish(span);
      span.finish();
    } else {
      LoggerFactory.getLogger(Flux.class)
          .error(
              "Failed to finish this.span, LettuceFluxTerminationRunnable cannot find this.span "
                  + "because it probably wasn't started.");
    }
  }

  @Override
  public void accept(final Signal signal) {
    SignalType signalType = signal.getType();
    if (signalType == SignalType.ON_ERROR || signalType == SignalType.ON_NEXT) {
      /*
      This signal involves processing a work unit - let's resume the span and
      attribute the work to that span.
      */
      span.finishThreadMigration();
    }
    if (signalType == SignalType.ON_COMPLETE || signalType == SignalType.ON_ERROR) {
      finishSpan(false, signal.getThrowable());
    } else if (signalType == SignalType.ON_NEXT) {
      ++numResults;
    }
  }

  @Override
  public void run() {
    if (span != null) {
      finishSpan(true, null);
    } else {
      LoggerFactory.getLogger(Flux.class)
          .error(
              "Failed to finish this.span to indicate cancellation, LettuceFluxTerminationRunnable"
                  + " cannot find this.span because it probably wasn't started.");
    }
  }

  public static class FluxOnSubscribeConsumer implements Consumer<Subscription> {

    private final LettuceFluxTerminationRunnable owner;
    private final RedisCommand command;
    private final boolean finishSpanOnClose;

    public FluxOnSubscribeConsumer(
        final LettuceFluxTerminationRunnable owner,
        final RedisCommand command,
        final boolean finishSpanOnClose) {
      this.owner = owner;
      this.command = command;
      this.finishSpanOnClose = finishSpanOnClose;
    }

    @Override
    public void accept(final Subscription subscription) {
      final AgentSpan span = startSpan(REDIS_QUERY);
      owner.span = span;
      DECORATE.afterStart(span);
      DECORATE.onCommand(span, command);
      if (finishSpanOnClose) {
        DECORATE.beforeFinish(span);
        span.finish();
      } else {
        /*
        The subscription does not really involve any 'work' - suspend the span so it can be
        correctly picked up by the subscriber later on.
         */
        span.startThreadMigration();
      }
    }
  }
}
