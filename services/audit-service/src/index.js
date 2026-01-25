const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PORT = process.env.PORT || '50052';
const BIND_ADDR = process.env.BIND_ADDR || `0.0.0.0:${PORT}`;

// const PROTO_PATH = path.join(__dirname, '..', '..', '..', '..', 'protos', 'audit.proto');
const PROTO_PATH = path.join(__dirname, '..', '..', '..', 'protos', 'audit.proto');//Fixed path (Change 1)

function loadProto() {
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(packageDef).audit;
}

async function main() {
  console.log('🚀 Starting Audit Service...');
  console.log(`📄 Loading proto: ${PROTO_PATH}`);

  const auditProto = loadProto();
  const server = new grpc.Server();

  server.addService(auditProto.AuditService.service, {
    LogEvent: (call, callback) => {
      const evt = call.request || {};

      // Immutable log (append-only behavior)
      console.log(
        `🧾 AUDIT EVENT: eventId=${evt.eventId} eventType=${evt.eventType} taskId=${evt.taskId} ` +
          `actorId=${evt.actorId} version=${evt.version} ts=${evt.timestamp}`
      );

      // Keep raw event JSON for traceability
      if (evt.rawEventJson) {
        console.log(`🧾 AUDIT RAW: ${evt.rawEventJson}`);
      }

      callback(null, { ok: true, message: 'event logged' });
    },
  });

  server.bindAsync(BIND_ADDR, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('❌ Failed to bind Audit Service gRPC server:', err);
      process.exit(1);
    }
    console.log(`✅ Audit Service listening on ${BIND_ADDR} (port ${port})`);
    server.start();
  });

  process.on('SIGTERM', () => {
    console.log('SIGTERM received, shutting down Audit Service...');
    server.tryShutdown(() => process.exit(0));
  });
}

main().catch((err) => {
  console.error('❌ Fatal error in Audit Service:', err);
  process.exit(1);
});

