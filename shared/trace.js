const { trace, context, propagation, SpanStatusCode } = require("@opentelemetry/api");
const tracer = trace.getTracer("order-platform");

function inject() {
  const carrier = {};
  propagation.inject(context.active(), carrier);
  return carrier.traceparent;
}

function withSpan(name, parentTp, fn, attributes = {}) {
  const parent = parentTp ? propagation.extract(context.active(), { traceparent: parentTp }) : context.active();
  return tracer.startActiveSpan(name, { attributes }, parent, async (span) => {
    try {
      return await fn();
    } catch (e) {
      span.recordException(e);
      span.setStatus({ code: SpanStatusCode.ERROR });
      throw e;
    } finally {
      span.end();
    }
  });
}

module.exports = { inject, withSpan };