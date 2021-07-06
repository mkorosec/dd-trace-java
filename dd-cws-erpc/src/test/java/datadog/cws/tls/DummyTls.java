
package datadog.cws.tls;

import datadog.cws.erpc.Request;
import datadog.cws.tls.CwsTls;

class DummyTls extends CwsTls {
    Request lastRequest;

    public DummyTls(int maxThread) {
        super(maxThread);
    }

    @Override
    public void sendRequest(Request request) {
        lastRequest = request;
    }
}