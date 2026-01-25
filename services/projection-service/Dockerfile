FROM node:18-alpine

WORKDIR /app

COPY services/projection-service/package.json ./
RUN npm install

COPY services/projection-service/ ./services/projection-service/
COPY protos/ ./protos/

WORKDIR /app/services/projection-service

EXPOSE 3001

CMD ["npm", "start"]
