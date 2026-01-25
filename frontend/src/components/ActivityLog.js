/**
 * ActivityLog Component
 * 
 * Displays activity log for a task
 * Updates in real-time via WebSocket
 */

import React, { useState, useEffect } from 'react';
import './ActivityLog.css';
import { api } from '../services/api';

function ActivityLog({ taskId }) {
  const [activities, setActivities] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (taskId) {
      fetchActivities();
    }
  }, [taskId]);

  const fetchActivities = async () => {
    try {
      setLoading(true);
      const task = await api.getTask(taskId);
      setActivities(task.activity || []);
    } catch (error) {
      console.error('Error fetching activities:', error);
    } finally {
      setLoading(false);
    }
  };

  if (!taskId) {
    return (
      <div className="activity-log">
        <div className="empty-state">Select a task to view activity</div>
      </div>
    );
  }

  return (
    <div className="activity-log">
      <div className="activity-log-header">
        <h2>Activity Log</h2>
        <button className="btn-refresh" onClick={fetchActivities} title="Refresh">
          🔄
        </button>
      </div>

      <div className="activity-log-content">
        {loading ? (
          <div className="loading">Loading activities...</div>
        ) : activities.length === 0 ? (
          <div className="empty">No activity yet.</div>
        ) : (
          <div className="activity-timeline">
            {activities.map((activity) => (
              <div key={activity.id} className="activity-item">
                <div className="activity-dot"></div>
                <div className="activity-content">
                  <p className="activity-description">{activity.description}</p>
                  <div className="activity-meta">
                    <span className="activity-actor">
                      {activity.actor_id || 'System'}
                    </span>
                    <span className="activity-time">
                      {new Date(activity.timestamp).toLocaleString()}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default ActivityLog;
