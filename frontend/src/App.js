/**
 * Main App Component
 * 
 * Real-Time Collaborative Workflow Engine Frontend
 * 
 * Features:
 * - Task list view
 * - Task detail view with real-time updates
 * - Activity log panel
 * - WebSocket integration for live updates
 * - Version conflict handling
 */

import React, { useState, useEffect } from 'react';
import './App.css';
import TaskList from './components/TaskList';
import TaskDetail from './components/TaskDetail';
import ActivityLog from './components/ActivityLog';
import { useWebSocket } from './hooks/useWebSocket';
import { api } from './services/api';

function App() {
  const [tasks, setTasks] = useState([]);
  const [selectedTask, setSelectedTask] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [conflictError, setConflictError] = useState(null);

  // WebSocket connection for real-time updates
  const ws = useWebSocket(process.env.REACT_APP_WS_URL || 'ws://localhost:4001');

  // Fetch tasks on mount
  useEffect(() => {
    fetchTasks();
  }, []);

  // Handle WebSocket messages
  useEffect(() => {
    if (!ws) return;

    const handleMessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        handleWebSocketUpdate(message);
      } catch (error) {
        console.error('Error parsing WebSocket message:', error);
      }
    };

    ws.addEventListener('message', handleMessage);

    return () => {
      ws.removeEventListener('message', handleMessage);
    };
  }, [ws, tasks, selectedTask]);

  /**
   * Fetch all tasks from API
   */
  const fetchTasks = async () => {
    try {
      setLoading(true);
      const data = await api.getTasks();
      setTasks(data);
      setError(null);
    } catch (err) {
      console.error('Error fetching tasks:', err);
      setError('Failed to load tasks. Please refresh the page.');
    } finally {
      setLoading(false);
    }
  };

  /**
   * Handle WebSocket updates
   */
  const handleWebSocketUpdate = (message) => {
    const { type, taskId, data } = message;

    switch (type) {
      case 'TASK_CREATED':
        // Add new task to list
        setTasks((prev) => [data, ...prev]);
        break;

      case 'TASK_STATUS_CHANGED':
        // Update task in list
        setTasks((prev) =>
          prev.map((task) =>
            task.id === taskId
              ? { ...task, status: data.status, version: data.version }
              : task
          )
        );

        // Update selected task if it's the one being updated
        if (selectedTask && selectedTask.id === taskId) {
          setSelectedTask((prev) => ({
            ...prev,
            status: data.status,
            version: data.version,
          }));
        }
        break;

      case 'COMMENT_ADDED':
        // Update selected task comments if it's the one being updated
        if (selectedTask && selectedTask.id === taskId) {
          fetchTaskDetail(taskId);
        }
        break;

      default:
        console.log('Unknown WebSocket message type:', type);
    }
  };

  /**
   * Fetch detailed task information
   */
  const fetchTaskDetail = async (taskId) => {
    try {
      const task = await api.getTask(taskId);
      setSelectedTask(task);
      setError(null);
    } catch (err) {
      console.error('Error fetching task detail:', err);
      setError('Failed to load task details.');
    }
  };

  /**
   * Handle task selection
   */
  const handleTaskSelect = (task) => {
    fetchTaskDetail(task.id);
    
    // Subscribe to WebSocket updates for this task
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'SUBSCRIBE_TASK',
        taskId: task.id,
      }));
    }
  };

  /**
   * Handle task creation
   */
  const handleCreateTask = async (title, description) => {
    try {
      setError(null);
      const result = await api.createTask(title, description);
      // Task will appear via WebSocket update
      console.log('Task creation accepted:', result);
    } catch (err) {
      console.error('Error creating task:', err);
      setError('Failed to create task. Please try again.');
    }
  };

  /**
   * Handle status update
   */
  const handleStatusUpdate = async (taskId, status, version) => {
    try {
      setError(null);
      setConflictError(null);
      await api.updateTaskStatus(taskId, status, version);
      // Update will come via WebSocket
    } catch (err) {
      if (err.response && err.response.status === 409) {
        // Version conflict
        setConflictError({
          message: 'Task was modified by another user',
          currentVersion: err.response.data.currentVersion,
        });
        // Refresh task to get latest version
        if (selectedTask && selectedTask.id === taskId) {
          fetchTaskDetail(taskId);
        }
      } else {
        console.error('Error updating task status:', err);
        setError('Failed to update task status. Please try again.');
      }
    }
  };

  /**
   * Handle comment addition
   */
  const handleAddComment = async (taskId, text) => {
    try {
      setError(null);
      await api.addComment(taskId, text);
      // Comment will appear via WebSocket update
    } catch (err) {
      console.error('Error adding comment:', err);
      setError('Failed to add comment. Please try again.');
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>Real-Time Collaborative Workflow Engine</h1>
        <p>Event-Driven, Local-First System</p>
      </header>

      {error && (
        <div className="error-banner">
          <span>⚠️ {error}</span>
          <button onClick={() => setError(null)}>✕</button>
        </div>
      )}

      {conflictError && (
        <div className="conflict-banner">
          <span>⚠️ Version Conflict: {conflictError.message}</span>
          <button onClick={() => setConflictError(null)}>✕</button>
        </div>
      )}

      <div className="app-content">
        <div className="app-sidebar">
          <TaskList
            tasks={tasks}
            loading={loading}
            onTaskSelect={handleTaskSelect}
            onCreateTask={handleCreateTask}
            selectedTaskId={selectedTask?.id}
          />
        </div>

        <div className="app-main">
          {selectedTask ? (
            <TaskDetail
              task={selectedTask}
              onStatusUpdate={handleStatusUpdate}
              onAddComment={handleAddComment}
              onRefresh={() => fetchTaskDetail(selectedTask.id)}
            />
          ) : (
            <div className="empty-state">
              <p>Select a task to view details</p>
            </div>
          )}
        </div>

        <div className="app-sidebar-right">
          {selectedTask && (
            <ActivityLog taskId={selectedTask.id} />
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
