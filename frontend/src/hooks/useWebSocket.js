/**
 * useWebSocket Hook
 * 
 * Manages WebSocket connection for real-time updates
 * Handles reconnection logic
 */

import { useState, useEffect, useRef } from 'react';

export function useWebSocket(url) {
  const [ws, setWs] = useState(null);
  const [connected, setConnected] = useState(false);
  const reconnectTimeoutRef = useRef(null);
  const reconnectAttempts = useRef(0);
  const maxReconnectAttempts = 10;

  useEffect(() => {
    if (!url) return;

    let websocket = null;
    let isMounted = true;

    const connect = () => {
      try {
        console.log(`🔌 Connecting to WebSocket: ${url}`);
        websocket = new WebSocket(url);

        websocket.onopen = () => {
          console.log('✅ WebSocket connected');
          if (isMounted) {
            setConnected(true);
            setWs(websocket);
            reconnectAttempts.current = 0;
          }
        };

        websocket.onclose = () => {
          console.log('❌ WebSocket disconnected');
          if (isMounted) {
            setConnected(false);
            setWs(null);
          }

          // Attempt to reconnect
          if (isMounted && reconnectAttempts.current < maxReconnectAttempts) {
            reconnectAttempts.current += 1;
            const delay = Math.min(1000 * Math.pow(2, reconnectAttempts.current), 30000);
            console.log(`🔄 Reconnecting in ${delay}ms (attempt ${reconnectAttempts.current})...`);
            
            reconnectTimeoutRef.current = setTimeout(() => {
              if (isMounted) {
                connect();
              }
            }, delay);
          } else if (reconnectAttempts.current >= maxReconnectAttempts) {
            console.error('❌ Max reconnection attempts reached');
          }
        };

        websocket.onerror = (error) => {
          console.error('❌ WebSocket error:', error);
        };

        websocket.onmessage = (event) => {
          // Messages are handled in the component
          console.log('📨 WebSocket message received');
        };
      } catch (error) {
        console.error('❌ Failed to create WebSocket:', error);
        if (isMounted) {
          setConnected(false);
        }
      }
    };

    connect();

    return () => {
      isMounted = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (websocket) {
        websocket.close();
      }
    };
  }, [url]);

  return ws;
}
