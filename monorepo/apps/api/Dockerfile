# syntax=docker/dockerfile:1.7

FROM node:22-alpine

WORKDIR /app

RUN corepack enable

COPY pnpm-lock.yaml pnpm-workspace.yaml package.json turbo.json tsconfig.json ./
COPY apps/api/package.json apps/api/package.json
COPY packages/db/package.json packages/db/package.json
COPY packages/dto/package.json packages/dto/package.json
COPY packages/tsconfig/package.json packages/tsconfig/package.json

RUN pnpm install --frozen-lockfile

COPY apps/api apps/api
COPY packages/db packages/db
COPY packages/dto packages/dto
COPY packages/tsconfig packages/tsconfig

WORKDIR /app/apps/api

EXPOSE 3000

CMD ["pnpm", "start"]
