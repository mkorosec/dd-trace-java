package datadog.cws.tls;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import datadog.cws.erpc.Erpc;
import datadog.cws.erpc.Request;
import datadog.trace.api.DDId;

/**
 * This class is as a thread local storage.
 *
 * <p>It associates and keep current threadId with spanId/traceId in a memory area that can be read
 * from the CWS eBPF code.
 */
public class CwsTls {
    public final static byte REGISTER_SPAN_TLS_OP = 6;
    final static int ENTRY_SIZE = Native.LONG_SIZE * 2;

    // Thread local storage
    private Pointer tls;
    private long maxThreads;

    public interface CLibrary extends Library {
        CLibrary Instance = (CLibrary) Native.load("c", CLibrary.class);

        NativeLong gettid();
    }

    private static int getTID() {
        return CLibrary.Instance.gettid().intValue();
    }

    public CwsTls(int maxThreads) {
        Memory tls = new Memory(maxThreads * ENTRY_SIZE);
        tls.clear();

        this.maxThreads = maxThreads;
        this.tls = tls;

        registerTls();
    }

    public Pointer getTlsPointer() {
        return tls;
    }

    private void registerTls() {
        Request request = new Request(REGISTER_SPAN_TLS_OP);
        Pointer pointer = request.getDataPointer();
        pointer.setLong(0, maxThreads);
        pointer.setPointer(Native.LONG_SIZE, tls);

        sendRequest(request);
    }

    public void sendRequest(Request request) {
        Erpc.send(request);
    }

    public long getSpanIdOffset(int threadId) {
        return threadId%maxThreads * ENTRY_SIZE;
    }

    public long getTraceIdOffset(int threadId) {
        return getSpanIdOffset(threadId) + Native.LONG_SIZE;
    }

    public void registerSpan(int threadId, DDId spanId, DDId traceId) {
        long spanIdOffset = getSpanIdOffset(threadId);
        long traceIdOffset = getTraceIdOffset(threadId);

        tls.setLong(spanIdOffset, spanId.toLong());
        tls.setLong(traceIdOffset, traceId.toLong());
    }

    public void registerSpan(DDId spanId, DDId traceId) {
        registerSpan(getTID(), spanId, traceId);
    }

    public DDId getSpanId(int threadId) {
        long offset = getSpanIdOffset(threadId);
        return DDId.from(tls.getLong(offset));
    }

    public DDId getSpanId() {
        return getSpanId(getTID());
    }

    public DDId getTraceId(int threadId) {
        long offset = getTraceIdOffset(threadId);
        return DDId.from(tls.getLong(offset));
    }

    public DDId getTraceId() {
        return getTraceId(getTID());
    }
}
