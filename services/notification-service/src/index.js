const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PORT = process.env.PORT || '50051';
const BIND_ADDR = process.env.BIND_ADDR || `0.0.0.0:${PORT}`;

// const PROTO_PATH = path.join(__dirname, '..', '..', '..', '..', 'protos', 'notification.proto');
const PROTO_PATH = path.join(__dirname, '..', '..', '..', 'protos', 'notification.proto');//Fixed path (Change 1)

function loadProto() {
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(packageDef).notification;
}

async function main() {
  console.log('🚀 Starting Notification Service...');
  console.log(`📄 Loading proto: ${PROTO_PATH}`);

  const notificationProto = loadProto();
  const server = new grpc.Server();

  server.addService(notificationProto.NotificationService.service, {
    SendTaskAlert: (call, callback) => {
      const alert = call.request || {};

      // Immutable log (append-only behavior)
      console.log(
        `🔔 TASK ALERT: eventType=${alert.eventType} taskId=${alert.taskId} ` +
          `previousStatus=${alert.previousStatus} newStatus=${alert.newStatus} ` +
          `actorId=${alert.actorId} version=${alert.version} ts=${alert.timestamp}`
      );

      callback(null, { ok: true, message: 'alert logged' });
    },
  });

  server.bindAsync(BIND_ADDR, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error('❌ Failed to bind Notification Service gRPC server:', err);
      process.exit(1);
    }
    console.log(`✅ Notification Service listening on ${BIND_ADDR} (port ${port})`);
    server.start();
  });

  process.on('SIGTERM', () => {
    console.log('SIGTERM received, shutting down Notification Service...');
    server.tryShutdown(() => process.exit(0));
  });
}

main().catch((err) => {
  console.error('❌ Fatal error in Notification Service:', err);
  process.exit(1);
});

