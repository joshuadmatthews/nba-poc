FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY index.js databricks.js rulesql.js ./
EXPOSE 4000
CMD ["node", "index.js"]
