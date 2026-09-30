# syntax=docker/dockerfile:1.7

# Build stage. The API used to run through tsx, compiling TypeScript on every
# start with the whole devDependency tree in the image.
FROM node:22-alpine AS builder

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

RUN pnpm --filter @well-simulation/api build

# Runtime stage. Only the bundle and the dependencies that cannot be bundled,
# which are the ones with native or platform-specific parts.
FROM node:22-alpine AS runner

WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/apps/api/dist ./dist

# A manifest written for the runtime rather than copied from the source tree.
# The API's own package.json declares workspace:* dependencies, which npm cannot
# resolve and which are already inside the bundle anyway. Only the packages
# marked external in tsup.config.ts are installed here.
RUN printf '%s\n' \
      '{' \
      '  "name": "well-simulation-api",' \
      '  "private": true,' \
      '  "type": "module",' \
      '  "dependencies": { "pg": "^8.16.3", "bcryptjs": "^3.0.3" }' \
      '}' > package.json \
    && npm install --omit=dev --no-package-lock \
    && npm cache clean --force

# Run as a non-root user. The base image ships one.
USER node

EXPOSE 3000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD node -e "fetch('http://127.0.0.1:3000/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "dist/index.js"]
