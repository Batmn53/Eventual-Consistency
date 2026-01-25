FROM node:18-alpine

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

EXPOSE 3000

# Set environment variable to allow external connections
ENV HOST=0.0.0.0
ENV WDS_SOCKET_HOST=localhost

# React dev server
CMD ["npm", "start"]
