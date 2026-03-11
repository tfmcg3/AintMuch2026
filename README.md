# Dutchie Dispensary Scraper

An Apify Actor that scrapes all cannabis dispensaries from [Dutchie.com](https://dutchie.com), the largest cannabis e-commerce platform in the US and Canada.

## Overview

This actor systematically crawls Dutchie.com to extract comprehensive dispensary data:

1. **Starts at `/cities`** - Extracts all city page URLs (~1,881 cities)
2. **Visits each city page** - Scrapes dispensary listings with infinite scroll handling
3. **Deduplicates by slug** - Ensures each dispensary appears only once
4. **Outputs multiple formats** - JSON (keyed by slug) and CSV

### Expected Output

- **5,000 - 10,000 unique dispensaries** across US and Canada
- **Coverage**: All US states with legal cannabis + Canadian provinces

## Output Schema

Each dispensary record contains:

| Field | Type | Description |
|-------|------|-------------|
| `cName` | string | Canonical name/slug (unique identifier) |
| `name` | string | Display name of the dispensary |
| `city_page` | string | URL of the city page where found |
| `state` | string | State/province code (e.g., CA, CO, BC) |
| `country` | string | Country code (US, CA, PR) |

### Sample Output (JSON keyed by slug)

```json
{
  "jungle-boys-dtla": {
    "cName": "jungle-boys-dtla",
    "name": "Jungle Boys - DTLA",
    "city_page": "https://dutchie.com/us/dispensaries/ca-los-angeles",
    "state": "CA",
    "country": "US"
  },
  "medmen-lax": {
    "cName": "medmen-lax",
    "name": "MedMen LAX",
    "city_page": "https://dutchie.com/us/dispensaries/ca-los-angeles",
    "state": "CA",
    "country": "US"
  }
}
```

## Input Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `maxCityPages` | integer | 0 | Max city pages to scrape (0 = all ~1,881) |
| `maxConcurrency` | integer | 5 | Concurrent browser instances (1-20) |
| `maxRequestRetries` | integer | 3 | Retry attempts for failed requests |
| `pageTimeout` | integer | 60000 | Page navigation timeout (ms) |
| `delayBetweenRequests` | integer | 1000 | Rate limiting delay (ms) |
| `startUrls` | array | [] | Custom URLs to scrape instead of all cities |
| `debugMode` | boolean | false | Enable verbose logging |
| `proxyConfiguration` | object | `{useApifyProxy: true}` | Proxy settings |

### Example Input (Full Scrape)

```json
{
  "maxCityPages": 0,
  "maxConcurrency": 10,
  "maxRequestRetries": 3,
  "pageTimeout": 60000,
  "delayBetweenRequests": 500,
  "debugMode": false,
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

### Example Input (Test Run - 5 Cities)

```json
{
  "maxCityPages": 5,
  "maxConcurrency": 2,
  "debugMode": true
}
```

### Example Input (Specific Cities Only)

```json
{
  "startUrls": [
    { "url": "https://dutchie.com/us/dispensaries/ca-los-angeles" },
    { "url": "https://dutchie.com/us/dispensaries/co-denver" },
    { "url": "https://dutchie.com/us/dispensaries/az-phoenix" }
  ],
  "maxConcurrency": 3
}
```

## Output Locations

After the actor runs, data is available in:

1. **Dataset** - Individual dispensary records (for export/API access)
2. **Key-Value Store**:
   - `dispensaries_by_slug` - JSON object keyed by slug
   - `dispensaries` - CSV format export (content-type: text/csv)
   - `summary` - Run statistics

## How It Works

### 1. City URL Extraction

The actor parses `/cities` to extract URLs in various formats:

- US: `/us/dispensaries/{state}-{city}` (e.g., `/us/dispensaries/ca-los-angeles`)
- Canada: `/ca/dispensaries/{province}-{city}` (e.g., `/ca/dispensaries/bc-vancouver`)
- Puerto Rico: `/dispensaries/-{city}` (e.g., `/dispensaries/-san-juan`)

### 2. Dispensary Extraction

For each city page:
- Handles age verification modal automatically
- Scrolls to load all dispensaries (infinite scroll)
- Extracts dispensary links matching `/dispensary/{slug}`
- Parses clean name from raw card text

### 3. Deduplication

Dispensaries are deduplicated by their URL slug (canonical name), ensuring each dispensary appears exactly once in the output even if listed on multiple city pages.

## Technical Details

### Built With

- **[Crawlee](https://crawlee.dev/)** - Modern web scraping framework
- **[Puppeteer](https://pptr.dev/)** - Headless Chrome automation
- **[Apify SDK](https://docs.apify.com/sdk/js)** - Actor runtime and storage

### Resource Requirements

| Setting | Minimum | Recommended |
|---------|---------|-------------|
| Memory | 2048 MB | 4096 MB |
| Timeout | 4 hours | 8 hours |
| Proxy | Required | Residential |

### Rate Limiting

The actor includes built-in rate limiting:
- Configurable delay between requests
- Request retry with exponential backoff
- Resource blocking (images, fonts) for faster loading

## Local Development

```bash
# Clone the repository
git clone https://github.com/tfmcg3/Creating-New-Apify-Actor.git
cd Creating-New-Apify-Actor

# Install dependencies
npm install

# Run locally (requires apify-cli)
apify run

# Or run with Node directly
npm start
```

## Deployment to Apify

### Option 1: Push via CLI

```bash
# Login to Apify
apify login

# Push the actor
apify push
```

### Option 2: GitHub Integration

1. Connect your GitHub repo to Apify Console
2. Apify will auto-build on push to main branch

### Option 3: Manual Upload

1. Go to [Apify Console](https://console.apify.com/)
2. Create new Actor
3. Upload the source files or connect Git repo

## Estimated Runtime

| Cities | Concurrency | Est. Time | Est. Cost |
|--------|-------------|-----------|-----------|
| 10 | 2 | ~5 min | ~$0.05 |
| 100 | 5 | ~30 min | ~$0.50 |
| 1,881 (all) | 10 | ~4-6 hours | ~$5-10 |

*Costs are approximate and depend on proxy usage and retry rates.*

## Troubleshooting

### Common Issues

1. **Age Verification Blocking**
   - The actor handles this automatically
   - If issues persist, try increasing `pageTimeout`

2. **Rate Limiting / Blocks**
   - Increase `delayBetweenRequests`
   - Use residential proxies
   - Reduce `maxConcurrency`

3. **Timeout Errors**
   - Increase `pageTimeout` to 90000+
   - Reduce `maxConcurrency`

4. **Missing Dispensaries**
   - Some cities may have no dispensaries
   - Check `summary` output for failure counts

## License

MIT License - Feel free to modify and distribute.

## Contributing

Pull requests welcome! Please ensure:
- Code follows existing style
- Add comments for complex logic
- Test with a small subset before full runs

## Support

- **Issues**: [GitHub Issues](https://github.com/tfmcg3/Creating-New-Apify-Actor/issues)
- **Apify Docs**: [docs.apify.com](https://docs.apify.com/)
