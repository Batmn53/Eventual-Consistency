/**
 * TaskDetail Component
 * 
 * Displays detailed task information
 * Allows updating status and adding comments
 * Handles version conflicts
 */

import React, { useState } from 'react';
import './TaskDetail.css';
import { api } from '../services/api';

function TaskDetail({ task, onStatusUpdate, onAddComment, onRefresh }) {
  const [commentText, setCommentText] = useState('');
  const [addingComment, setAddingComment] = useState(false);

  const handleStatusChange = async (newStatus) => {
    if (newStatus === task.status) return;
    await onStatusUpdate(task.id, newStatus, task.version);
  };

  const handleAddComment = async (e) => {
    e.preventDefault();
    if (!commentText.trim()) return;

    setAddingComment(true);
    try {
      await onAddComment(task.id, commentText.trim());
      setCommentText('');
    } catch (error) {
      console.error('Error adding comment:', error);
    } finally {
      setAddingComment(false);
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

  const statusOptions = ['TODO', 'IN_PROGRESS', 'DONE'];

  return (
    <div className="task-detail">
      <div className="task-detail-header">
        <div>
          <h1>{task.title}</h1>
          <div className="task-meta">
            <span className="task-version">Version {task.version}</span>
            <span className="task-date">
              Updated {new Date(task.updated_at).toLocaleString()}
            </span>
          </div>
        </div>
        <button className="btn-refresh" onClick={onRefresh} title="Refresh">
          🔄
        </button>
      </div>

      <div className="task-detail-content">
        <div className="task-section">
          <h3>Description</h3>
          <p>{task.description || 'No description provided.'}</p>
        </div>

        <div className="task-section">
          <h3>Status</h3>
          <div className="status-selector">
            {statusOptions.map((status) => (
              <button
                key={status}
                className={`status-option ${task.status === status ? 'active' : ''}`}
                style={{
                  backgroundColor:
                    task.status === status ? getStatusColor(status) : '#ecf0f1',
                  color: task.status === status ? 'white' : '#333',
                }}
                onClick={() => handleStatusChange(status)}
              >
                {status}
              </button>
            ))}
          </div>
        </div>

        <div className="task-section">
          <h3>Comments ({task.comments?.length || 0})</h3>
          <div className="comments-list">
            {task.comments && task.comments.length > 0 ? (
              task.comments.map((comment) => (
                <div key={comment.id} className="comment-item">
                  <div className="comment-header">
                    <span className="comment-author">
                      {comment.actor_id || 'Anonymous'}
                    </span>
                    <span className="comment-date">
                      {new Date(comment.created_at).toLocaleString()}
                    </span>
                  </div>
                  <p className="comment-text">{comment.text}</p>
                </div>
              ))
            ) : (
              <p className="empty-comments">No comments yet.</p>
            )}
          </div>

          <form className="comment-form" onSubmit={handleAddComment}>
            <textarea
              placeholder="Add a comment..."
              value={commentText}
              onChange={(e) => setCommentText(e.target.value)}
              disabled={addingComment}
              rows="3"
            />
            <button type="submit" disabled={addingComment || !commentText.trim()}>
              {addingComment ? 'Adding...' : 'Add Comment'}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default TaskDetail;
