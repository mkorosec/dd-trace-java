package datadog.trace.instrumentation.lettuce5.rx;

import static datadog.trace.instrumentation.lettuce5.LettuceInstrumentationUtil.expectsResponse;

import io.lettuce.core.protocol.RedisCommand;
import java.util.function.Supplier;
import net.bytebuddy.asm.Advice;
import reactor.core.publisher.Mono;

public class LettuceMonoCreationAdvice {

  @Advice.OnMethodEnter(suppress = Throwable.class)
  public static RedisCommand extractCommandName(
      @Advice.Argument(0) final Supplier<RedisCommand> supplier) {
    return supplier.get();
  }

  // throwables wouldn't matter here, because no spans have been started due to redis command not
  // being run until the user subscribes to the Mono publisher
  @Advice.OnMethodExit(suppress = Throwable.class)
  public static void monitorSpan(
      @Advice.Enter final RedisCommand command,
      @Advice.Return(readOnly = false) Mono<?> publisher) {
    final boolean finishSpanOnClose = !expectsResponse(command);
    final LettuceMonoDualConsumer mdc = new LettuceMonoDualConsumer(command, finishSpanOnClose);
    publisher = publisher.doOnSubscribe(mdc);
    // register the 'work' callback such that a proper 'resume span' checkpoint can be emitted
    publisher = publisher.doOnNext(mdc);
    // register the call back to close the span only if necessary
    if (!finishSpanOnClose) {
      publisher = publisher.doOnSuccessOrError(mdc);
    }
  }
}
