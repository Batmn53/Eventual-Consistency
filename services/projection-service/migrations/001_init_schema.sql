-- PostgreSQL Schema for Read Models
-- This is the ONLY database schema in the system
-- All data here is derived from Kafka events

-- Tasks table (read model)
CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY,
  title VARCHAR(500) NOT NULL,
  description TEXT,
  status VARCHAR(50) NOT NULL DEFAULT 'TODO',
  version INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  created_by VARCHAR(255)
);

-- Task activity log (read model)
CREATE TABLE IF NOT EXISTS task_activity (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  description TEXT NOT NULL,
  actor_id VARCHAR(255),
  timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Comments table (read model)
CREATE TABLE IF NOT EXISTS task_comments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  text TEXT NOT NULL,
  actor_id VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_updated_at ON tasks(updated_at);
CREATE INDEX IF NOT EXISTS idx_task_activity_task_id ON task_activity(task_id);
CREATE INDEX IF NOT EXISTS idx_task_activity_timestamp ON task_activity(timestamp);
CREATE INDEX IF NOT EXISTS idx_task_comments_task_id ON task_comments(task_id);
