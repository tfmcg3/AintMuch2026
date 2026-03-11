# Use Apify's Puppeteer image as base
FROM apify/actor-node-puppeteer-chrome:18

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --omit=dev --omit=optional \
    && npm cache clean --force

# Copy source code
COPY . ./

# Run the actor
CMD npm start
