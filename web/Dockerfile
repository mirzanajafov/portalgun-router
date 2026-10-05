FROM node:24-bookworm-slim AS build
RUN npm install -g pnpm@11.21.0
WORKDIR /app/web
COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY web ./
COPY bench/results /app/bench/results
ENV NEXT_TELEMETRY_DISABLED=1
RUN pnpm build

FROM node:24-bookworm-slim
ENV NODE_ENV=production PORT=3000 HOSTNAME=0.0.0.0 NEXT_TELEMETRY_DISABLED=1
WORKDIR /app
COPY --from=build /app/web/.next/standalone ./
COPY --from=build /app/web/.next/static ./.next/static
USER node
EXPOSE 3000
CMD ["node", "server.js"]
