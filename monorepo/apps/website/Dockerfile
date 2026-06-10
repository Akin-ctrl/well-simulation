# syntax=docker/dockerfile:1.7

FROM node:22-alpine AS builder

WORKDIR /app

RUN corepack enable

# Copy workspace manifests first for better Docker layer caching.
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json turbo.json tsconfig.json ./

# Copy app and package manifests used by the website app.
COPY apps/website/package.json apps/website/package.json
COPY packages/db/package.json packages/db/package.json
COPY packages/dto/package.json packages/dto/package.json
COPY packages/eslint-config/package.json packages/eslint-config/package.json
COPY packages/tsconfig/package.json packages/tsconfig/package.json
COPY packages/typescript-config/package.json packages/typescript-config/package.json
COPY packages/ui/package.json packages/ui/package.json
COPY packages/utils/package.json packages/utils/package.json
COPY apps/api/package.json apps/api/package.json

RUN pnpm install --frozen-lockfile

# Copy remaining source files and build only the dashboard app.
COPY . .
RUN pnpm --filter @corsight/website exec react-router build

FROM nginx:1.27-alpine AS runner

COPY apps/website/nginx.conf /etc/nginx/conf.d/default.conf
RUN rm -rf /usr/share/nginx/html/*
COPY --from=builder /app/apps/website/build/client /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
