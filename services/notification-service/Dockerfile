FROM node:18-alpine

WORKDIR /app

COPY services/notification-service/package.json ./
RUN npm install

COPY services/notification-service/ ./services/notification-service/
COPY protos/ ./protos/

WORKDIR /app/services/notification-service

EXPOSE 50051

CMD ["npm", "start"]

