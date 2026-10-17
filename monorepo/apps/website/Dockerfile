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
COPY packages/ui/package.json packages/ui/package.json
COPY packages/utils/package.json packages/utils/package.json
COPY apps/api/package.json apps/api/package.json

RUN pnpm install --frozen-lockfile

# Copy remaining source files and build only the dashboard app.
COPY . .
RUN pnpm --filter @well-simulation/website exec react-router build

# Fill the CSP with hashes of this build's inline startup scripts. Fails the
# image build if they cannot be found.
RUN node apps/website/scripts/csp-inline-script-hashes.mjs \
      apps/website/build/client/index.html \
      apps/website/nginx.conf \
      /app/nginx.conf

# nginx 1.27.5, including dynamic upstream DNS support added in 1.27.3.
FROM nginx@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10 AS runner

COPY --from=builder /app/nginx.conf /etc/nginx/conf.d/default.conf
RUN rm -rf /usr/share/nginx/html/*
COPY --from=builder /app/apps/website/build/client /usr/share/nginx/html

RUN nginx -t

# Readiness includes the route used by the browser and the API's database check.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -T 2 -O /dev/null http://127.0.0.1/ \
      && wget -q -T 2 -O /dev/null http://127.0.0.1/api/ready || exit 1

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
