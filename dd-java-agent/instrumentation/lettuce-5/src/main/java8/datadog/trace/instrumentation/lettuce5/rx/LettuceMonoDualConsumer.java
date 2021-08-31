package datadog.trace.instrumentation.lettuce5.rx;

import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.activateSpan;
import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.activeSpan;
import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.startSpan;
import static datadog.trace.instrumentation.lettuce5.LettuceClientDecorator.DECORATE;
import static datadog.trace.instrumentation.lettuce5.LettuceClientDecorator.REDIS_QUERY;

import datadog.trace.bootstrap.instrumentation.api.AgentSpan;
import datadog.trace.context.TraceScope;
import io.lettuce.core.protocol.RedisCommand;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class LettuceMonoDualConsumer<R, T, U extends Throwable>
    implements Consumer<R>, BiConsumer<T, Throwable> {

  private AgentSpan span = null;
  private final RedisCommand command;
  private final boolean finishSpanOnClose;
  private final AgentSpan parentSpan;

  public LettuceMonoDualConsumer(final RedisCommand command, final boolean finishSpanOnClose) {
    this.command = command;
    this.finishSpanOnClose = finishSpanOnClose;
    parentSpan = activeSpan();
  }

  @Override
  public void accept(final R r) {
    if (span == null) {
      // 'onSubscribe'
      TraceScope parentScope = null;
      try {
        if (parentSpan != null) {
          parentScope = activateSpan(parentSpan);
        }
        span = startSpan(REDIS_QUERY);
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
      } finally {
        if (parentScope != null) {
          parentScope.close();
        }
      }
    } else {
      // 'onNext'
      /*
      The work is delivered to the subscriber here - need to resume the span to properly attribute it.
      This is a Mono consumer - meaning that there will be exactly one work item delivered or
      there would be an error signalled. Therefore, we don't need to be 'suspending' the span
      after the work item has been processed - instead the LettuceFluxTerminationRunnable will take
      care of finishing the span.
      */
      span.finishThreadMigration();
    }
  }

  @Override
  public void accept(final T t, final Throwable throwable) {
    if (span != null) {
      DECORATE.onError(span, throwable);
      DECORATE.beforeFinish(span);
      span.finishThreadMigration();
      span.finish();
    } else {
      LoggerFactory.getLogger(Mono.class)
          .error(
              "Failed to finish this.span, BiConsumer cannot find this.span because "
                  + "it probably wasn't started.");
    }
  }
}
