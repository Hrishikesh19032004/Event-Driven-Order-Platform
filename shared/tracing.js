const { NodeSDK } = require("@opentelemetry/sdk-node");
const { OTLPTraceExporter } = require("@opentelemetry/exporter-trace-otlp-http");

module.exports = (serviceName) => {
  const sdk = new NodeSDK({
    serviceName,
    traceExporter: new OTLPTraceExporter({ url: "http://localhost:4318/v1/traces" }),
  });
  sdk.start();
  process.on("SIGINT", () => sdk.shutdown().finally(() => process.exit(0)));
};