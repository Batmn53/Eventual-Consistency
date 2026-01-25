# Real-Time Collaborative Workflow Engine

A local-first, event-driven system for real-time collaborative task management. Built with Docker, Kafka, PostgreSQL, Redis, and WebSockets.

## 🎯 Project Overview

This is a **real-world collaborative system** where:
- Multiple users work on the same tasks concurrently
- All changes are visible in real-time
- No updates are silently overwritten
- Every change is recorded as a historical event
- The system remains correct under concurrent edits

**This is NOT a CRUD app. This is an event-driven, real-time system.**

## 🏗️ Architecture

### Core Principles

1. **Kafka is the Source of Truth**
   - Every task change is an immutable Kafka event
   - PostgreSQL contains ONLY derived state (read models)
   - No service may mutate state without Kafka

2. **REST APIs DO NOT UPDATE DATABASES**
   - REST endpoints only validate intent
   - REST endpoints ONLY emit Kafka events
   - No direct Postgres writes from REST handlers

3. **Optimistic Concurrency Control**
   - Each task has a version
   - Clients MUST send `If-Version` (via version field)
   - Version mismatch → HTTP 409 Conflict
   - NEVER auto-merge conflicting updates

4. **Real-Time is a Side Effect**
   - WebSocket updates come ONLY from event consumers
   - Never push WebSocket messages directly from REST APIs
   - Correctness > Convenience

### System Components

```
┌─────────────┐
│  Frontend   │ (React)
│  :3000      │
└──────┬──────┘
       │ REST API
       │ WebSocket
       ▼
┌─────────────┐
│ API Gateway │ (Node.js + Express)
│  :4000      │
└──────┬──────┘
       │
       │ Emits Events
       ▼
┌─────────────┐
│   Kafka     │ (Event Log - Source of Truth)
│  :9092      │
└──────┬──────┘
       │
       │ Consumes Events
       ▼
┌─────────────┐
│ Projection  │ (Kafka Consumer)
│  Service    │
└──────┬──────┘
       │
       ├──► PostgreSQL (Read Models)
       │
       └──► Redis Pub/Sub
              │
              ▼
       ┌─────────────┐
       │  WebSocket  │ (Real-Time Updates)
       │  Service    │
       │  :4001      │
       └─────────────┘
```

### Services

#### 1. API Gateway (`services/api-gateway`)
- **Port**: 4000
- **Responsibilities**:
  - JWT authentication (mock)
  - Authorization checks
  - Input validation
  - Accept user commands
  - Emit Kafka events (NO direct database writes)

**Endpoints**:
- `POST /api/tasks` - Create task
- `GET /api/tasks` - List all tasks
- `GET /api/tasks/:id` - Get task details
- `PATCH /api/tasks/:id/status` - Update task status (with version check)
- `POST /api/tasks/:id/comments` - Add comment
- `GET /health` - Health check

#### 2. Projection Service (`services/projection-service`)
- **Responsibilities**:
  - Consume Kafka events from `task-events` topic
  - Update PostgreSQL read models (ONLY service allowed to write to Postgres)
  - Publish Redis Pub/Sub messages
  - Handle duplicate events safely (idempotent)

**Event Types**:
- `TASK_CREATED`
- `TASK_STATUS_CHANGED`
- `COMMENT_ADDED`

#### 3. WebSocket Service (`services/websocket-service`)
- **Port**: 4001
- **Responsibilities**:
  - Maintain WebSocket connections with clients
  - Subscribe to Redis Pub/Sub for task updates
  - Broadcast updates to connected clients
  - Handle presence (who is viewing/editing)

**Rules**:
- Stateless - horizontally scalable
- No database access
- Updates come ONLY from Redis Pub/Sub

#### 4. Frontend (`frontend`)
- **Port**: 3000
- **Technology**: React (functional components)
- **Features**:
  - Task list view
  - Task detail view with real-time updates
  - Activity log panel
  - WebSocket integration for live updates
  - Version conflict handling

### Data Flow

#### Creating a Task

1. User submits task via Frontend
2. Frontend → API Gateway (`POST /api/tasks`)
3. API Gateway validates input
4. API Gateway emits `TASK_CREATED` event to Kafka
5. API Gateway returns `202 Accepted`
6. Projection Service consumes event
7. Projection Service writes to PostgreSQL
8. Projection Service publishes to Redis Pub/Sub
9. WebSocket Service receives Redis message
10. WebSocket Service broadcasts to connected clients
11. Frontend receives WebSocket update
12. Frontend updates UI

#### Updating Task Status

1. User changes status via Frontend
2. Frontend sends current version with request
3. API Gateway reads current version from PostgreSQL
4. API Gateway checks version match
5. If mismatch → `409 Conflict`
6. If match → emit `TASK_STATUS_CHANGED` event to Kafka
7. Follow steps 6-12 from above

## 🚀 Getting Started

### Prerequisites

- Docker
- Docker Compose

**NO local installation required!** Everything runs in Docker containers.

### Running the System

1. **Clone and navigate to the project**:
   ```bash
   cd "Eventual Consistency"
   ```

2. **Start all services**:
   ```bash
   docker-compose up --build
   ```

   This single command starts:
   - Zookeeper
   - Kafka
   - PostgreSQL
   - Redis
   - API Gateway
   - Projection Service
   - Notification Service (gRPC)
   - Audit Service (gRPC)
   - WebSocket Service
   - Frontend

3. **Access the application**:
   - Frontend: http://localhost:3000
   - API Gateway: http://localhost:4000
   - WebSocket Server: ws://localhost:4001

### Development Mode with Hot Reload

The system is configured with **Docker Compose Watch** for automatic hot-reloading during development. Code changes are automatically reflected in running containers without manual rebuilds.

**To use watch mode:**

```bash
docker compose watch
```

Or with build:

```bash
docker compose up --build --watch
```

**How it works:**
- Source code directories are mounted as volumes for instant file access
- Services use `nodemon` (Node.js) or built-in hot-reload (React) to automatically restart on file changes
- Docker Compose Watch syncs file changes and triggers container updates
- Changes to `.js`, `.jsx`, `.css`, and other source files are detected automatically

**Services with hot-reload:**
- ✅ API Gateway (nodemon)
- ✅ Projection Service (nodemon)
- ✅ Notification Service (nodemon)
- ✅ Audit Service (nodemon)
- ✅ WebSocket Service (nodemon)
- ✅ Frontend (React Fast Refresh)

**Note:** For production builds, use `docker-compose up --build` without watch mode.

### Health Checks

- API Gateway: http://localhost:4000/health

## 📁 Project Structure

```
.
├── docker-compose.yml          # Orchestration for all services
├── services/
│   ├── api-gateway/           # REST API service
│   │   ├── src/
│   │   │   └── index.js       # Main API Gateway code
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── projection-service/    # Kafka consumer
│   │   ├── src/
│   │   │   └── index.js       # Event processing logic
│   │   ├── migrations/
│   │   │   └── 001_init_schema.sql
│   │   ├── Dockerfile
│   │   └── package.json
│   └── websocket-service/     # WebSocket server
│       ├── src/
│       │   └── index.js       # WebSocket handling
│       ├── Dockerfile
│       └── package.json
├── frontend/                  # React application
│   ├── src/
│   │   ├── components/        # React components
│   │   ├── hooks/             # Custom hooks
│   │   ├── services/          # API client
│   │   └── App.js
│   ├── Dockerfile
│   └── package.json
└── README.md
```

## 🔧 Configuration

### Environment Variables

All services use environment variables configured in `docker-compose.yml`:

- **Kafka**: `KAFKA_BROKER=kafka:29092`
- **PostgreSQL**: Configured via `POSTGRES_*` variables
- **Redis**: Configured via `REDIS_*` variables
- **Frontend**: `REACT_APP_API_URL`, `REACT_APP_WS_URL`

### Database Schema

PostgreSQL schema is automatically initialized from `services/projection-service/migrations/001_init_schema.sql`.

**Tables**:
- `tasks` - Task read model
- `task_activity` - Activity log
- `task_comments` - Task comments

## 🧪 Testing the System

### 1. Create a Task

```bash
curl -X POST http://localhost:4000/api/tasks \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer mock-user-1" \
  -d '{
    "title": "Test Task",
    "description": "This is a test task"
  }'
```

### 2. List Tasks

```bash
curl http://localhost:4000/api/tasks
```

### 3. Update Task Status

```bash
curl -X PATCH http://localhost:4000/api/tasks/{taskId}/status \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer mock-user-1" \
  -d '{
    "status": "IN_PROGRESS",
    "version": 1
  }'
```

### 4. Test Version Conflict

Try updating the same task with an old version number. You should receive a `409 Conflict` response.

## 🐛 Troubleshooting

### Services not starting

1. Check Docker is running
2. Check ports are not in use:
   - 3000 (Frontend)
   - 4000 (API Gateway)
   - 4001 (WebSocket)
   - 5432 (PostgreSQL)
   - 6379 (Redis)
   - 9092 (Kafka)
   - 2181 (Zookeeper)

### Kafka connection issues

- Ensure Zookeeper starts before Kafka
- Check Kafka logs: `docker logs kafka`

### Database connection issues

- Wait for PostgreSQL to fully initialize
- Check PostgreSQL logs: `docker logs postgres`

### WebSocket not connecting

- Check WebSocket service logs: `docker logs websocket-service`
- Verify Redis is running: `docker logs redis`

## 📝 Development Notes

### Event Schema

All events follow this structure:

```json
{
  "eventId": "uuid",
  "eventType": "TASK_CREATED | TASK_STATUS_CHANGED | COMMENT_ADDED",
  "taskId": "string",
  "payload": {},
  "actorId": "string",
  "version": number,
  "timestamp": number
}
```

### Version Conflict Handling

When a version conflict occurs:
1. API Gateway returns `409 Conflict`
2. Frontend displays warning
3. Frontend auto-refreshes task to get latest version
4. User can retry action

### Idempotency

The projection service handles duplicate events safely:
- Checks if event already processed
- Skips processing if version already exists
- Ensures exactly-once semantics per event

## 🎓 Key Concepts

### Event Sourcing

All state changes are stored as immutable events in Kafka. PostgreSQL contains derived read models for fast queries.

### CQRS (Command Query Responsibility Segregation)

- **Commands** (writes) go through Kafka events
- **Queries** (reads) go directly to PostgreSQL

### Optimistic Concurrency Control

Clients include version numbers with updates. Server rejects updates with stale versions, preventing lost updates.

### Real-Time Updates

WebSocket updates are a side effect of event processing, ensuring consistency across all clients.

## 📚 Further Reading

- [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
- [CQRS](https://martinfowler.com/bliki/CQRS.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

## 🔒 Security Notes

- JWT authentication is mocked for development
- In production, implement proper authentication
- Use environment variables for secrets
- Enable HTTPS for production

## 📄 License

This project is for educational purposes.

---

**Built with correctness in mind. Every architectural decision prioritizes data integrity and system reliability over convenience.**
