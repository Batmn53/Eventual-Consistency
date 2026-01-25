/**
 * API Gateway Service
 * 
 * Responsibilities:
 * - JWT authentication (mock)
 * - Authorization checks
 * - Input validation
 * - Accept user commands
 * - Emit Kafka events (NO direct database writes)
 * 
 * CRITICAL RULE: This service MUST NOT write to PostgreSQL.
 * All state changes go through Kafka events only.
 */

const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const { body, param, validationResult } = require('express-validator');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 4000;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:29092';

// PostgreSQL connection pool (READ ONLY - for fetching read models)
const pgPool = new Pool({
  host: process.env.POSTGRES_HOST || 'postgres',
  port: process.env.POSTGRES_PORT || 5432,
  user: process.env.POSTGRES_USER || 'workflow_user',
  password: process.env.POSTGRES_PASSWORD || 'workflow_pass',
  database: process.env.POSTGRES_DB || 'workflow_db',
  max: 10,
});

// Initialize Kafka producer
const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: [KAFKA_BROKER],
  retry: {
    // retries: 5,
    // initialRetryTime: 100,
    retries: 15,//Fixed (Change 1)
    initialRetryTime: 1000,//Fixed (Change 1)
  },
});

const producer = kafka.producer();
let producerReady = false;

// Initialize producer connection
async function initKafka() {
  try {
    await producer.connect();
    producerReady = true;
    console.log('✅ Kafka producer connected');
  } catch (error) {
    console.error('❌ Failed to connect to Kafka:', error);
    process.exit(1);
  }
}

initKafka();

// Middleware
app.use(cors());
app.use(express.json());

// Mock JWT authentication middleware
const authenticate = (req, res, next) => {
  // In production, verify JWT token from Authorization header
  // For now, extract from header or use default mock user
  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    req.userId = authHeader.split(' ')[1] || 'mock-user-1';
  } else {
    req.userId = 'mock-user-1'; // Default mock user
  }
  next();
};

app.use(authenticate);

// Validation error handler
const handleValidationErrors = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }
  next();
};

/**
 * Emit Kafka event
 * This is the ONLY way state changes are made in the system
 */
async function emitEvent(eventType, taskId, payload, actorId, version = null) {
  if (!producerReady) {
    throw new Error('Kafka producer not ready');
  }

  const event = {
    eventId: uuidv4(),
    eventType,
    taskId,
    payload,
    actorId,
    version,
    timestamp: Date.now(),
  };

  try {
    await producer.send({
      topic: 'task-events',
      messages: [
        {
          key: taskId, // Partition by taskId for ordering
          value: JSON.stringify(event),
        },
      ],
    });
    console.log(`📤 Event emitted: ${eventType} for task ${taskId}`);
    return event;
  } catch (error) {
    console.error('❌ Failed to emit Kafka event:', error);
    throw error;
  }
}

/**
 * POST /api/tasks
 * Create a new task
 * 
 * Flow:
 * 1. Validate input
 * 2. Emit TASK_CREATED event to Kafka
 * 3. Return 202 Accepted (event is being processed)
 */
app.post(
  '/api/tasks',
  [
    body('title').trim().notEmpty().withMessage('Title is required'),
    body('description').optional().isString(),
  ],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { title, description } = req.body;
      const taskId = uuidv4();
      const actorId = req.userId;

      // Emit event to Kafka (NO database write here!)
      await emitEvent('TASK_CREATED', taskId, {
        title,
        description: description || '',
        status: 'TODO',
      }, actorId, 1); // Initial version is 1

      res.status(202).json({
        message: 'Task creation request accepted',
        taskId,
        version: 1,
      });
    } catch (error) {
      console.error('Error creating task:', error);
      res.status(503).json({
        error: 'Service unavailable',
        message: 'Failed to process task creation. Kafka may be unavailable.',
      });
    }
  }
);

/**
 * GET /api/tasks
 * Get all tasks (with optional filtering)
 */
app.get('/api/tasks', async (req, res) => {
  try {
    const { status } = req.query;
    let query = 'SELECT id, title, description, status, version, created_at, updated_at, created_by FROM tasks';
    const params = [];

    if (status) {
      query += ' WHERE status = $1';
      params.push(status);
    }

    query += ' ORDER BY updated_at DESC';

    const result = await pgPool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching tasks:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /api/tasks/:id
 * Get task details
 * 
 * This reads from PostgreSQL (read model)
 * The projection service maintains this data
 */
app.get(
  '/api/tasks/:id',
  [param('id').isUUID().withMessage('Invalid task ID')],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { id } = req.params;

      // Read from PostgreSQL (read model maintained by projection service)
      const result = await pgPool.query(
        `SELECT 
          t.id, t.title, t.description, t.status, t.version, 
          t.created_at, t.updated_at, t.created_by,
          (SELECT COUNT(*) FROM task_comments WHERE task_id = t.id) as comment_count
         FROM tasks t
         WHERE t.id = $1`,
        [id]
      );

      if (result.rows.length === 0) {
        return res.status(404).json({ error: 'Task not found' });
      }

      const task = result.rows[0];

      // Fetch comments
      const commentsResult = await pgPool.query(
        `SELECT id, text, actor_id, created_at
         FROM task_comments
         WHERE task_id = $1
         ORDER BY created_at ASC`,
        [id]
      );

      // Fetch activity log
      const activityResult = await pgPool.query(
        `SELECT id, description, actor_id, timestamp
         FROM task_activity
         WHERE task_id = $1
         ORDER BY timestamp DESC
         LIMIT 50`,
        [id]
      );

      res.json({
        ...task,
        comments: commentsResult.rows,
        activity: activityResult.rows,
      });
    } catch (error) {
      console.error('Error fetching task:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  }
);

/**
 * PATCH /api/tasks/:id/status
 * Update task status
 * 
 * Flow:
 * 1. Validate input and version
 * 2. Check current version from PostgreSQL (optimistic concurrency control)
 * 3. If version mismatch, return 409 Conflict
 * 4. Emit TASK_STATUS_CHANGED event to Kafka
 * 5. Return 202 Accepted
 * 
 * CRITICAL: Version conflict handling (409 Conflict)
 */
app.patch(
  '/api/tasks/:id/status',
  [
    param('id').isUUID().withMessage('Invalid task ID'),
    body('status').isIn(['TODO', 'IN_PROGRESS', 'DONE']).withMessage('Invalid status'),
    body('version').isInt({ min: 1 }).withMessage('Version must be a positive integer'),
  ],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { id } = req.params;
      const { status, version } = req.body;
      const actorId = req.userId;

      // Get current task version from PostgreSQL (read model)
      const currentTask = await pgPool.query(
        'SELECT id, version, status FROM tasks WHERE id = $1',
        [id]
      );

      if (currentTask.rows.length === 0) {
        return res.status(404).json({ error: 'Task not found' });
      }

      const currentVersion = currentTask.rows[0].version;

      // Optimistic concurrency control: check version match
      if (version !== currentVersion) {
        return res.status(409).json({
          error: 'Version conflict',
          message: 'Task has been modified by another user',
          currentVersion,
          providedVersion: version,
        });
      }

      // Version matches, emit event to Kafka
      await emitEvent('TASK_STATUS_CHANGED', id, {
        status,
      }, actorId, version + 1); // Increment version

      res.status(202).json({
        message: 'Status update request accepted',
        taskId: id,
        version: version + 1,
      });
    } catch (error) {
      console.error('Error updating task status:', error);
      res.status(503).json({
        error: 'Service unavailable',
        message: 'Failed to process status update. Kafka may be unavailable.',
      });
    }
  }
);

/**
 * POST /api/tasks/:id/comments
 * Add a comment to a task
 * 
 * Flow:
 * 1. Validate input
 * 2. Emit COMMENT_ADDED event to Kafka
 * 3. Return 202 Accepted
 */
app.post(
  '/api/tasks/:id/comments',
  [
    param('id').isUUID().withMessage('Invalid task ID'),
    body('text').trim().notEmpty().withMessage('Comment text is required'),
  ],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { id } = req.params;
      const { text } = req.body;
      const actorId = req.userId;

      // Emit event to Kafka
      await emitEvent('COMMENT_ADDED', id, {
        text,
      }, actorId);

      res.status(202).json({
        message: 'Comment addition request accepted',
        taskId: id,
      });
    } catch (error) {
      console.error('Error adding comment:', error);
      res.status(503).json({
        error: 'Service unavailable',
        message: 'Failed to process comment. Kafka may be unavailable.',
      });
    }
  }
);

// Health check
app.get('/health', async (req, res) => {
  let postgresStatus = 'unknown';
  try {
    await pgPool.query('SELECT 1');
    postgresStatus = 'connected';
  } catch (error) {
    postgresStatus = 'disconnected';
  }

  res.json({
    status: 'ok',
    kafka: producerReady ? 'connected' : 'disconnected',
    postgres: postgresStatus,
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing connections...');
  await producer.disconnect();
  await pgPool.end();
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`🚀 API Gateway running on port ${PORT}`);
  console.log(`📡 Kafka broker: ${KAFKA_BROKER}`);
});
