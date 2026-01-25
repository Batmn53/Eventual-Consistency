/**
 * API Service
 * 
 * Handles all REST API calls to the API Gateway
 */

import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:4000';

// Renamed the raw axios instance to avoid export conflict
const axiosInstance = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token if available (mock for now)
axiosInstance.interceptors.request.use((config) => {
  const token = localStorage.getItem('auth_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

const api = {
  /**
   * Get all tasks
   */
  getTasks: async (status = null) => {
    const params = status ? { status } : {};
    const response = await axiosInstance.get('/api/tasks', { params });
    return response.data;
  },

  /**
   * Get task by ID
   */
  getTask: async (taskId) => {
    const response = await axiosInstance.get(`/api/tasks/${taskId}`);
    return response.data;
  },

  /**
   * Create a new task
   */
  createTask: async (title, description = '') => {
    const response = await axiosInstance.post('/api/tasks', { title, description });
    return response.data;
  },

  /**
   * Update task status
   */
  updateTaskStatus: async (taskId, status, version) => {
    const response = await axiosInstance.patch(`/api/tasks/${taskId}/status`, {
      status,
      version,
    });
    return response.data;
  },

  /**
   * Add comment to task
   */
  addComment: async (taskId, text) => {
    const response = await axiosInstance.post(`/api/tasks/${taskId}/comments`, { text });
    return response.data;
  },
};

export { api };
