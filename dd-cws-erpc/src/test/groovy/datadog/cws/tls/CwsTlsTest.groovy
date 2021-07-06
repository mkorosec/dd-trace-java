package datadog.cws.tls

import static org.junit.Assert.assertEquals

import com.sun.jna.Native
import com.sun.jna.Pointer

import datadog.cws.erpc.Request
import datadog.cws.tls.CwsTls
import datadog.cws.tls.DummyTls
import datadog.trace.api.DDId
import datadog.trace.test.util.DDSpecification

class CwsTlsTest extends DDSpecification {
    def "register trace and span to tls"() {
        setup:
        DummyTls tls = new DummyTls(1000)

        when:
        DDId spanId = DDId.from(456L)
        DDId traceId = DDId.from(789L)
        tls.registerSpan(123, spanId, traceId)

        then:
        tls.getSpanId(123) == spanId
        tls.getTraceId(123) == traceId
        tls.getSpanId(111) == DDId.from(0L)
        tls.getTraceId(222) ==  DDId.from(0L)
    }


    def "register tls"(){
        when:
        DummyTls tls = new DummyTls(1000)

        then:
        tls.lastRequest.getOpCode() == tls.REGISTER_SPAN_TLS_OP
        tls.lastRequest.getDataPointer().getLong(0) == 1000
        tls.lastRequest.getDataPointer().getPointer(Native.LONG_SIZE) == tls.getTlsPointer()
    }
}