# Stage 1: install prod deps
FROM node:22.22.2-alpine AS deps
WORKDIR /app

RUN apk add --no-cache bash

ENV CI=true

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

# Stage 2: build
FROM node:22.22.2-alpine AS build
WORKDIR /app

RUN apk add --no-cache bash

COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# Stage 3: runtime
FROM node:22.22.2-alpine
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000

COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./

EXPOSE 3000
USER node
CMD ["npm", "start"]
