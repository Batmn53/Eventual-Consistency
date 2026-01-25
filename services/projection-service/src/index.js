/**
 * Projection Service
 * 
 * Responsibilities:
 * - Consume Kafka events from task-events topic
 * - Update PostgreSQL read models (ONLY service allowed to write to Postgres)
 * - Publish Redis Pub/Sub messages for WebSocket fan-out
 * - Handle duplicate events safely (idempotent)
 * 
 * CRITICAL RULES:
 * 1. This is the ONLY service that writes to PostgreSQL
 * 2. All updates must be idempotent (handle duplicate events)
 * 3. Events are processed in order per task (partitioned by taskId)
 * 4. Version must increment strictly by 1
 */

const { Kafka } = require('kafkajs');
const { Pool } = require('pg');
const { createClient } = require('redis');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';
const POSTGRES_HOST = process.env.POSTGRES_HOST || 'postgres';
const POSTGRES_PORT = process.env.POSTGRES_PORT || 5432;
const POSTGRES_USER = process.env.POSTGRES_USER || 'workflow_user';
const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || 'workflow_pass';
const POSTGRES_DB = process.env.POSTGRES_DB || 'workflow_db';
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;

// gRPC (internal only)
const NOTIFICATION_GRPC_ADDR = process.env.NOTIFICATION_GRPC_ADDR || 'notification-service:50051';
const AUDIT_GRPC_ADDR = process.env.AUDIT_GRPC_ADDR || 'audit-service:50052';
const GRPC_DEADLINE_MS = Number(process.env.GRPC_DEADLINE_MS || 1500);

// const NOTIFICATION_PROTO_PATH = path.join(__dirname, '..', '..', '..', '..', 'protos', 'notification.proto');
// const AUDIT_PROTO_PATH = path.join(__dirname, '..', '..', '..', '..', 'protos', 'audit.proto');
const NOTIFICATION_PROTO_PATH = path.join(__dirname, '..', '..', '..', 'protos', 'notification.proto');//Fixed path (Change 1)
const AUDIT_PROTO_PATH = path.join(__dirname, '..', '..', '..', 'protos', 'audit.proto');//Fixed path (Change 1)

let notificationClient = null; 
let auditClient = null;

function initGrpcClients() {
  try {
    const loaderOptions = {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    };

    const notificationDef = protoLoader.loadSync(NOTIFICATION_PROTO_PATH, loaderOptions);
    const notificationPkg = grpc.loadPackageDefinition(notificationDef).notification;

    const auditDef = protoLoader.loadSync(AUDIT_PROTO_PATH, loaderOptions);
    const auditPkg = grpc.loadPackageDefinition(auditDef).audit;

    notificationClient = new notificationPkg.NotificationService(
      NOTIFICATION_GRPC_ADDR,
      grpc.credentials.createInsecure()
    );

    auditClient = new auditPkg.AuditService(AUDIT_GRPC_ADDR, grpc.credentials.createInsecure());

    console.log('✅ gRPC clients initialized');
    console.log(`   - NotificationService: ${NOTIFICATION_GRPC_ADDR}`);
    console.log(`   - AuditService: ${AUDIT_GRPC_ADDR}`);
  } catch (error) {
    // Fail loudly but keep the service alive to continue projections.
    console.error('❌ Failed to initialize gRPC clients (internal side-effects disabled):', error);
    notificationClient = null;
    auditClient = null;
  }
}

function grpcDeadline() {
  return new Date(Date.now() + GRPC_DEADLINE_MS);
}

async function grpcAuditLogEvent(event, rawEventJson) {
  if (!auditClient) return;

  const request = {
    eventId: String(event.eventId || ''),
    eventType: String(event.eventType || ''),
    taskId: String(event.taskId || ''),
    actorId: String(event.actorId || ''),
    version: Number(event.version || 0),
    timestamp: Number(event.timestamp || Date.now()),
    rawEventJson: String(rawEventJson || ''),
  };

  return new Promise((resolve) => {
    auditClient.LogEvent(request, { deadline: grpcDeadline() }, (err, res) => {
      if (err) {
        console.error('❌ gRPC AuditService.LogEvent failed:', {
          message: err.message,
          code: err.code,
          details: err.details,
          service: AUDIT_GRPC_ADDR,
          eventType: request.eventType,
          taskId: request.taskId,
        });
        return resolve(false);
      }
      resolve(Boolean(res && res.ok));
    });
  });
}

async function grpcNotifyTaskStatusChanged(event, previousStatus) {
  if (!notificationClient) return;

  const request = {
    eventType: 'TASK_STATUS_CHANGED',
    taskId: String(event.taskId || ''),
    actorId: String(event.actorId || ''),
    version: Number(event.version || 0),
    timestamp: Number(event.timestamp || Date.now()),
    previousStatus: String(previousStatus || ''),
    newStatus: String((event.payload && event.payload.status) || ''),
  };

  return new Promise((resolve) => {
    notificationClient.SendTaskAlert(request, { deadline: grpcDeadline() }, (err, res) => {
      if (err) {
        console.error('❌ gRPC NotificationService.SendTaskAlert failed:', {
          message: err.message,
          code: err.code,
          details: err.details,
          service: NOTIFICATION_GRPC_ADDR,
          taskId: request.taskId,
          version: request.version,
        });
        return resolve(false);
      }
      resolve(Boolean(res && res.ok));
    });
  });
}

// Initialize PostgreSQL connection pool
const pgPool = new Pool({
  host: POSTGRES_HOST,
  port: POSTGRES_PORT,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
  database: POSTGRES_DB,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Initialize Redis client
let redisClient = null;
let redisPublisher = null;

async function initRedis() {
  try {
    redisClient = createClient({
      socket: {
        host: REDIS_HOST,
        port: REDIS_PORT,
      },
    });
    redisPublisher = redisClient.duplicate();

    await redisClient.connect();
    await redisPublisher.connect();
    console.log('✅ Redis connected');
  } catch (error) {
    console.error('❌ Failed to connect to Redis:', error);
    // Continue without Redis (real-time updates will be disabled)
  }
}

// Initialize Kafka consumer
const kafka = new Kafka({
  clientId: 'projection-service',
  brokers: [KAFKA_BROKER],
  retry: {
    // retries: 5,
    // initialRetryTime: 100,
    retries: 15,//Fixed (Change 1)
    initialRetryTime: 1000,//Fixed (Change 1)
  },
});

const consumer = kafka.consumer({
  groupId: 'projection-service-group',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

/**
 * Publish update to Redis Pub/Sub for WebSocket fan-out
 */
async function publishToRedis(channel, message) {
  if (redisPublisher) {
    try {
      await redisPublisher.publish(channel, JSON.stringify(message));
      console.log(`📢 Published to Redis: ${channel}`);
    } catch (error) {
      console.error('❌ Failed to publish to Redis:', error);
      // Continue processing even if Redis fails
    }
  }
}

/**
 * Process TASK_CREATED event
 */
async function handleTaskCreated(event) {
  const { taskId, payload, actorId, version } = event;

  try {
    // Check if task already exists (idempotency)
    const existingTask = await pgPool.query(
      'SELECT id, version FROM tasks WHERE id = $1',
      [taskId]
    );

    if (existingTask.rows.length > 0) {
      console.log(`⚠️  Task ${taskId} already exists, skipping (idempotent)`);
      return;
    }

    // Insert new task
    await pgPool.query(
      `INSERT INTO tasks (id, title, description, status, version, created_by, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())`,
      [taskId, payload.title, payload.description || '', payload.status || 'TODO', version, actorId]
    );

    // Log activity
    await pgPool.query(
      `INSERT INTO task_activity (task_id, description, actor_id, timestamp)
       VALUES ($1, $2, $3, NOW())`,
      [taskId, `Task created: ${payload.title}`, actorId]
    );

    // Publish to Redis for real-time updates
    await publishToRedis('task-updates', {
      type: 'TASK_CREATED',
      taskId,
      data: {
        id: taskId,
        title: payload.title,
        description: payload.description,
        status: payload.status,
        version,
      },
    });

    console.log(`✅ Task created: ${taskId}`);
  } catch (error) {
    console.error(`❌ Error processing TASK_CREATED for ${taskId}:`, error);
    throw error;
  }
}

/**
 * Process TASK_STATUS_CHANGED event
 */
async function handleTaskStatusChanged(event) {
  const { taskId, payload, actorId, version } = event;

  try {
    // Get current task state
    const currentTask = await pgPool.query(
      'SELECT id, version, status FROM tasks WHERE id = $1',
      [taskId]
    );

    if (currentTask.rows.length === 0) {
      console.error(`❌ Task ${taskId} not found for status update`);
      return;
    }

    const currentVersion = currentTask.rows[0].version;
    const currentStatus = currentTask.rows[0].status;

    // Idempotency check: if version is already processed, skip
    if (currentVersion >= version) {
      console.log(`⚠️  Event version ${version} already processed for task ${taskId} (current: ${currentVersion})`);
      return;
    }

    // Version must increment by exactly 1 (optimistic concurrency control)
    if (version !== currentVersion + 1) {
      console.error(`❌ Version mismatch for task ${taskId}: expected ${currentVersion + 1}, got ${version}`);
      // In production, might want to handle this differently (e.g., dead letter queue)
      return;
    }

    // Update task status
    await pgPool.query(
      `UPDATE tasks 
       SET status = $1, version = $2, updated_at = NOW()
       WHERE id = $3`,
      [payload.status, version, taskId]
    );

    // Log activity
    await pgPool.query(
      `INSERT INTO task_activity (task_id, description, actor_id, timestamp)
       VALUES ($1, $2, $3, NOW())`,
      [taskId, `Status changed: ${currentStatus} → ${payload.status}`, actorId]
    );

    // Internal side-effect: send task alert (gRPC) AFTER successful Postgres update
    // Fail loudly in logs, but never block projections.
    await grpcNotifyTaskStatusChanged(event, currentStatus);

    // Publish to Redis for real-time updates
    await publishToRedis('task-updates', {
      type: 'TASK_STATUS_CHANGED',
      taskId,
      data: {
        id: taskId,
        status: payload.status,
        version,
      },
    });

    console.log(`✅ Task status updated: ${taskId} → ${payload.status} (v${version})`);
  } catch (error) {
    console.error(`❌ Error processing TASK_STATUS_CHANGED for ${taskId}:`, error);
    throw error;
  }
}

/**
 * Process COMMENT_ADDED event
 */
async function handleCommentAdded(event) {
  const { taskId, payload, actorId } = event;

  try {
    // Verify task exists
    const taskExists = await pgPool.query('SELECT id FROM tasks WHERE id = $1', [taskId]);
    if (taskExists.rows.length === 0) {
      console.error(`❌ Task ${taskId} not found for comment`);
      return;
    }

    // Insert comment
    const result = await pgPool.query(
      `INSERT INTO task_comments (task_id, text, actor_id, created_at)
       VALUES ($1, $2, $3, NOW())
       RETURNING id`,
      [taskId, payload.text, actorId]
    );

    const commentId = result.rows[0].id;

    // Log activity
    await pgPool.query(
      `INSERT INTO task_activity (task_id, description, actor_id, timestamp)
       VALUES ($1, $2, $3, NOW())`,
      [taskId, `Comment added`, actorId]
    );

    // Publish to Redis for real-time updates
    await publishToRedis('task-updates', {
      type: 'COMMENT_ADDED',
      taskId,
      data: {
        commentId,
        text: payload.text,
        actorId,
      },
    });

    console.log(`✅ Comment added to task ${taskId}`);
  } catch (error) {
    console.error(`❌ Error processing COMMENT_ADDED for ${taskId}:`, error);
    throw error;
  }
}

/**
 * Process Kafka event
 */
async function processEvent(eventData) {
  try {
    const event = JSON.parse(eventData);
    const { eventType, taskId } = event;

    console.log(`📥 Processing event: ${eventType} for task ${taskId}`);

    switch (eventType) {
      case 'TASK_CREATED':
        await handleTaskCreated(event);
        break;
      case 'TASK_STATUS_CHANGED':
        await handleTaskStatusChanged(event);
        break;
      case 'COMMENT_ADDED':
        await handleCommentAdded(event);
        break;
      default:
        console.warn(`⚠️  Unknown event type: ${eventType}`);
    }

    // Internal side-effect: immutable audit trail for every processed event
    // (runs only after handler succeeds)
    await grpcAuditLogEvent(event, eventData);
  } catch (error) {
    console.error('❌ Error processing event:', error);
    // In production, might want to send to dead letter queue
    throw error;
  }
}

/**
 * Main consumer loop
 */
async function startConsumer() {
  try {
    await consumer.connect();
    console.log('✅ Kafka consumer connected');

    await consumer.subscribe({
      topic: 'task-events',
      fromBeginning: false, // Start from latest offset
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const eventData = message.value.toString();
          await processEvent(eventData);
        } catch (error) {
          console.error('❌ Error in consumer message handler:', error);
          // Continue processing other messages
        }
      },
    });

    console.log('✅ Consumer started, listening for events...');
  } catch (error) {
    console.error('❌ Failed to start consumer:', error);
    process.exit(1);
  }
}

/**
 * Test PostgreSQL connection
 */
async function testPostgresConnection() {
  try {
    const result = await pgPool.query('SELECT NOW()');
    console.log('✅ PostgreSQL connected:', result.rows[0].now);
  } catch (error) {
    console.error('❌ Failed to connect to PostgreSQL:', error);
    process.exit(1);
  }
}

/**
 * Initialize and start the service
 */
async function start() {
  console.log('🚀 Starting Projection Service...');
  initGrpcClients();
  
  // Test PostgreSQL connection
  await testPostgresConnection();
  
  // Initialize Redis
  await initRedis();
  
  // Start Kafka consumer
  await startConsumer();
  
  console.log('✅ Projection Service ready');
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down...');
  await consumer.disconnect();
  await pgPool.end();
  if (redisClient) await redisClient.quit();
  if (redisPublisher) await redisPublisher.quit();
  process.exit(0);
});

// Start the service
start().catch((error) => {
  console.error('❌ Fatal error:', error);
  process.exit(1);
});
