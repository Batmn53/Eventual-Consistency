FROM node:18-alpine

WORKDIR /app

COPY services/audit-service/package.json ./
RUN npm install

COPY services/audit-service/ ./services/audit-service/
COPY protos/ ./protos/

WORKDIR /app/services/audit-service

EXPOSE 50052

CMD ["npm", "start"]

