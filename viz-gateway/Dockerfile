# Stage 1: build the Three.js viewer
FROM node:22-alpine AS viewer-build
WORKDIR /repo

# Install zod at repo root so shared/ can resolve it
# (shared/validation.ts imports zod; Rollup resolves from /repo/node_modules up)
RUN npm install --no-save zod@^3.23.8

# Install viewer deps
COPY viewer/package.json viewer/
RUN cd viewer && npm install

# Copy shared types (imported as @shared/)
COPY shared/ shared/

# Copy viewer source and build
COPY viewer/ viewer/
RUN cd viewer && npm run build
# Output: /repo/viewer/dist/

# Stage 2: viz-gateway runtime
FROM node:22-alpine
WORKDIR /app

COPY viz-gateway/package.json ./
RUN npm install --omit=dev

COPY viz-gateway/viz-gateway.js ./

# Copy built viewer into gateway's dist/ (served at http://localhost:8090/)
COPY --from=viewer-build /repo/viewer/dist ./dist

CMD ["node", "viz-gateway.js"]
