/**
 * WebSocket Service
 * 
 * Responsibilities:
 * - Maintain WebSocket connections with clients
 * - Subscribe to Redis Pub/Sub for task updates
 * - Broadcast updates to connected clients
 * - Handle presence (who is viewing/editing)
 * 
 * CRITICAL RULES:
 * 1. Stateless - horizontally scalable
 * 2. No database access
 * 3. Updates come ONLY from Redis Pub/Sub (from projection service)
 * 4. Never push messages directly from REST APIs
 */

const WebSocket = require('ws');
const http = require('http');
const { createClient } = require('redis');

const PORT = process.env.PORT || 4001;
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;

// Create HTTP server
const server = http.createServer();

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Store active connections
const clients = new Map(); // Map<ws, { userId, subscribedTasks }>

// Initialize Redis subscriber
let redisSubscriber = null;

async function initRedis() {
  try {
    redisSubscriber = createClient({
      socket: {
        host: REDIS_HOST,
        port: REDIS_PORT,
      },
    });

    await redisSubscriber.connect();
    console.log('✅ Redis subscriber connected');

    // Subscribe to task updates channel
    await redisSubscriber.subscribe('task-updates', (message) => {
      try {
        const update = JSON.parse(message);
        broadcastUpdate(update);
      } catch (error) {
        console.error('❌ Error parsing Redis message:', error);
      }
    });

    console.log('✅ Subscribed to Redis channel: task-updates');
  } catch (error) {
    console.error('❌ Failed to connect to Redis:', error);
    // Service can still accept connections, but won't receive updates
  }
}

/**
 * Broadcast update to all relevant clients
 */
function broadcastUpdate(update) {
  const { taskId, type, data } = update;
  const message = JSON.stringify({
    type,
    taskId,
    data,
    timestamp: Date.now(),
  });

  let sentCount = 0;
  clients.forEach((clientInfo, ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      // Send to all clients subscribed to this task, or all clients if no specific subscription
      if (!clientInfo.subscribedTasks || clientInfo.subscribedTasks.has(taskId)) {
        try {
          ws.send(message);
          sentCount++;
        } catch (error) {
          console.error('❌ Error sending message to client:', error);
        }
      }
    }
  });

  console.log(`📢 Broadcasted ${type} for task ${taskId} to ${sentCount} client(s)`);
}

/**
 * Handle WebSocket connection
 */
wss.on('connection', (ws, req) => {
  const clientId = `client-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  console.log(`✅ New WebSocket connection: ${clientId}`);

  // Initialize client info
  clients.set(ws, {
    userId: null,
    subscribedTasks: new Set(),
    clientId,
  });

  // Send welcome message
  ws.send(JSON.stringify({
    type: 'CONNECTED',
    clientId,
    timestamp: Date.now(),
  }));

  // Handle incoming messages
  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      const clientInfo = clients.get(ws);

      switch (message.type) {
        case 'SUBSCRIBE_TASK':
          // Client wants to subscribe to updates for a specific task
          if (message.taskId) {
            clientInfo.subscribedTasks.add(message.taskId);
            ws.send(JSON.stringify({
              type: 'SUBSCRIBED',
              taskId: message.taskId,
            }));
            console.log(`📌 Client ${clientId} subscribed to task ${message.taskId}`);
          }
          break;

        case 'UNSUBSCRIBE_TASK':
          // Client wants to unsubscribe from a task
          if (message.taskId) {
            clientInfo.subscribedTasks.delete(message.taskId);
            ws.send(JSON.stringify({
              type: 'UNSUBSCRIBED',
              taskId: message.taskId,
            }));
            console.log(`📌 Client ${clientId} unsubscribed from task ${message.taskId}`);
          }
          break;

        case 'PING':
          // Heartbeat
          ws.send(JSON.stringify({ type: 'PONG' }));
          break;

        default:
          console.warn(`⚠️  Unknown message type: ${message.type}`);
      }
    } catch (error) {
      console.error('❌ Error processing WebSocket message:', error);
      ws.send(JSON.stringify({
        type: 'ERROR',
        message: 'Invalid message format',
      }));
    }
  });

  // Handle connection close
  ws.on('close', () => {
    console.log(`❌ WebSocket connection closed: ${clientId}`);
    clients.delete(ws);
  });

  // Handle errors
  ws.on('error', (error) => {
    console.error(`❌ WebSocket error for ${clientId}:`, error);
    clients.delete(ws);
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`🚀 WebSocket Service running on port ${PORT}`);
  initRedis();
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down...');
  
  // Close all WebSocket connections
  clients.forEach((clientInfo, ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
  });

  // Close Redis connection
  if (redisSubscriber) {
    await redisSubscriber.quit();
  }

  server.close(() => {
    console.log('✅ WebSocket Service shut down');
    process.exit(0);
  });
});
