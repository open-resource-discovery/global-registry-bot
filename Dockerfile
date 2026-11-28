# Stage 1: install prod deps
FROM node:26.13.1-alpine@sha256:6bd9ab43c0ec0190ca7b2d193906cb8b7edf6e7f4afa43ddeccd6452710bffb5 AS deps
WORKDIR /app

RUN apk add --no-cache bash

ENV CI=true

COPY package.json package-lock.json .npmrc ./
RUN npm install --global --ignore-scripts npm@11.21.3 && npm ci --omit=dev --ignore-scripts

# Stage 2: build
FROM node:26.13.1-alpine@sha256:6bd9ab43c0ec0190ca7b2d193906cb8b7edf6e7f4afa43ddeccd6452710bffb5 AS build
WORKDIR /app

RUN apk add --no-cache bash

COPY package.json package-lock.json .npmrc ./
RUN npm install --global --ignore-scripts npm@11.21.3 && npm ci --ignore-scripts
COPY . .
RUN npm run build

# Stage 3: runtime
FROM node:26.13.1-alpine@sha256:6bd9ab43c0ec0190ca7b2d193906cb8b7edf6e7f4afa43ddeccd6452710bffb5
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000

COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json .npmrc ./

EXPOSE 3000
USER node
CMD ["npm", "start"]
