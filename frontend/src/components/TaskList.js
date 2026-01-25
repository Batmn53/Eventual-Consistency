/**
 * TaskList Component
 * 
 * Displays list of all tasks
 * Allows creating new tasks
 */

import React, { useState } from 'react';
import './TaskList.css';

function TaskList({ tasks, loading, onTaskSelect, onCreateTask, selectedTaskId }) {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState('');
  const [creating, setCreating] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!title.trim()) return;

    setCreating(true);
    try {
      await onCreateTask(title.trim(), description.trim());
      setTitle('');
      setDescription('');
      setShowCreateForm(false);
    } catch (error) {
      console.error('Error creating task:', error);
    } finally {
      setCreating(false);
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'TODO':
        return '#95a5a6';
      case 'IN_PROGRESS':
        return '#3498db';
      case 'DONE':
        return '#2ecc71';
      default:
        return '#95a5a6';
    }
  };

  return (
    <div className="task-list">
      <div className="task-list-header">
        <h2>Tasks</h2>
        <button
          className="btn-create"
          onClick={() => setShowCreateForm(!showCreateForm)}
        >
          + New Task
        </button>
      </div>

      {showCreateForm && (
        <form className="create-task-form" onSubmit={handleSubmit}>
          <input
            type="text"
            placeholder="Task title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            disabled={creating}
            required
          />
          <textarea
            placeholder="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            disabled={creating}
            rows="3"
          />
          <div className="form-actions">
            <button type="submit" disabled={creating || !title.trim()}>
              {creating ? 'Creating...' : 'Create'}
            </button>
            <button
              type="button"
              onClick={() => {
                setShowCreateForm(false);
                setTitle('');
                setDescription('');
              }}
              disabled={creating}
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      <div className="task-list-content">
        {loading ? (
          <div className="loading">Loading tasks...</div>
        ) : tasks.length === 0 ? (
          <div className="empty">No tasks yet. Create one to get started!</div>
        ) : (
          tasks.map((task) => (
            <div
              key={task.id}
              className={`task-item ${selectedTaskId === task.id ? 'selected' : ''}`}
              onClick={() => onTaskSelect(task)}
            >
              <div className="task-item-header">
                <h3>{task.title}</h3>
                <span
                  className="status-badge"
                  style={{ backgroundColor: getStatusColor(task.status) }}
                >
                  {task.status}
                </span>
              </div>
              {task.description && (
                <p className="task-description">{task.description}</p>
              )}
              <div className="task-item-footer">
                <span className="task-version">v{task.version}</span>
                <span className="task-date">
                  {new Date(task.updated_at).toLocaleDateString()}
                </span>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export default TaskList;
