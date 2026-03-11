/**
 * Dutchie Dispensary Scraper - Apify Actor
 * 
 * Scrapes all dispensaries from Dutchie.com by:
 * 1. Extracting all city page URLs from dutchie.com/cities
 * 2. Visiting each city page and extracting dispensary info
 * 3. Deduplicating by slug
 * 4. Outputting in JSON and CSV formats
 */

import { Actor, log } from 'apify';
import { PuppeteerCrawler, RequestQueue } from 'crawlee';

// Constants
const BASE_URL = 'https://dutchie.com';
const CITIES_URL = `${BASE_URL}/cities`;

// Store for deduplication
const dispensaryMap = new Map();

/**
 * Parse location info from city page URL
 * URL patterns:
 * - /dispensaries/-cityname (Puerto Rico)
 * - /ca/dispensaries/ab-cityname (Canada - province code)
 * - /us/dispensaries/ca-los-angeles (US - state code)
 */
function parseLocationFromUrl(url) {
    const urlObj = new URL(url, BASE_URL);
    const pathname = urlObj.pathname;
    
    // Pattern: /ca/dispensaries/XX-city or /us/dispensaries/XX-city
    const countryMatch = pathname.match(/^\/(us|ca)\/dispensaries\/([a-z]{2})-(.+)$/);
    if (countryMatch) {
        return {
            country: countryMatch[1].toUpperCase(),
            state: countryMatch[2].toUpperCase(),
            citySlug: countryMatch[3]
        };
    }
    
    // Pattern: /dispensaries/-cityname (Puerto Rico) or /dispensaries/XX-city
    const prMatch = pathname.match(/^\/dispensaries\/(-)?([a-z]{2})?-?(.+)$/);
    if (prMatch) {
        if (prMatch[1] === '-') {
            // Puerto Rico pattern: /dispensaries/-cityname
            return {
                country: 'PR',
                state: 'PR',
                citySlug: prMatch[3]
            };
        } else if (prMatch[2]) {
            // Old US pattern: /dispensaries/XX-city
            return {
                country: 'US',
                state: prMatch[2].toUpperCase(),
                citySlug: prMatch[3]
            };
        }
    }
    
    return {
        country: 'UNKNOWN',
        state: 'UNKNOWN',
        citySlug: pathname.split('/').pop()
    };
}

/**
 * Extract dispensary name from card text
 * The text often contains extra info like "Pickup available • 2 Miles away"
 */
function extractDispensaryName(rawText) {
    if (!rawText) return '';
    
    // Split on common delimiters and take the first part
    const cleanName = rawText
        .split(/(?:Pickup|Delivery|Ready|\xa0|•)/)[0]
        .trim();
    
    return cleanName;
}

/**
 * Main Actor function
 */
Actor.main(async () => {
    // Get input configuration
    const input = await Actor.getInput() || {};
    const {
        maxCityPages = 0,  // 0 = scrape all cities
        maxConcurrency = 5,
        maxRequestRetries = 3,
        pageTimeout = 60000,
        delayBetweenRequests = 1000,
        startUrls = [],
        debugMode = false,
        proxyConfiguration = { useApifyProxy: true }
    } = input;

    log.setLevel(debugMode ? log.LEVELS.DEBUG : log.LEVELS.INFO);
    log.info('Starting Dutchie Dispensary Scraper');
    log.info(`Configuration: maxCityPages=${maxCityPages}, maxConcurrency=${maxConcurrency}`);

    // Initialize proxy
    const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

    // Create request queue
    const requestQueue = await RequestQueue.open();

    // Track city pages for reporting
    let citiesProcessed = 0;
    let citiesFailed = 0;
    const cityUrls = [];

    // Create the crawler
    const crawler = new PuppeteerCrawler({
        requestQueue,
        proxyConfiguration: proxyConfig,
        maxConcurrency,
        maxRequestRetries,
        navigationTimeoutSecs: pageTimeout / 1000,
        requestHandlerTimeoutSecs: pageTimeout / 1000 * 2,
        
        // Pre-navigation hook
        preNavigationHooks: [
            async ({ page, request }) => {
                // Set viewport
                await page.setViewport({ width: 1920, height: 1080 });
                
                // Block unnecessary resources for faster loading
                await page.setRequestInterception(true);
                page.on('request', (req) => {
                    const resourceType = req.resourceType();
                    if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                        req.abort();
                    } else {
                        req.continue();
                    }
                });
            }
        ],

        // Request handler
        requestHandler: async ({ page, request, enqueueLinks }) => {
            const url = request.url;
            log.info(`Processing: ${url}`);

            // Handle age verification modal if present
            try {
                await page.waitForSelector('button', { timeout: 5000 });
                const buttons = await page.$$('button');
                for (const button of buttons) {
                    const text = await page.evaluate(el => el.textContent, button);
                    if (text && text.trim().toUpperCase() === 'YES') {
                        await button.click();
                        await page.waitForTimeout(2000);
                        break;
                    }
                }
            } catch (e) {
                // Modal not present or already dismissed, continue
            }

            // Handle different page types
            if (url.includes('/cities')) {
                // CITIES PAGE: Extract all city URLs
                log.info('Processing cities index page');
                
                await page.waitForSelector('a[href*="/dispensaries/"]', { timeout: 30000 });
                
                const cityLinks = await page.evaluate(() => {
                    const links = document.querySelectorAll('a[href*="/dispensaries/"]');
                    return Array.from(links)
                        .map(a => a.getAttribute('href'))
                        .filter(href => 
                            href && 
                            (href.includes('/us/dispensaries/') || 
                             href.includes('/ca/dispensaries/') || 
                             href.match(/\/dispensaries\/[a-z-]+$/))
                        )
                        .map(href => href.startsWith('http') ? href : `https://dutchie.com${href}`);
                });

                // Deduplicate city links
                const uniqueCityLinks = [...new Set(cityLinks)];
                log.info(`Found ${uniqueCityLinks.length} unique city pages`);

                // Enqueue city pages
                const citiesToProcess = maxCityPages > 0 
                    ? uniqueCityLinks.slice(0, maxCityPages) 
                    : uniqueCityLinks;
                
                for (const cityUrl of citiesToProcess) {
                    cityUrls.push(cityUrl);
                    await requestQueue.addRequest({
                        url: cityUrl,
                        userData: { pageType: 'city' }
                    });
                }
                
                log.info(`Enqueued ${citiesToProcess.length} city pages for scraping`);

            } else if (url.includes('/dispensaries/')) {
                // CITY PAGE: Extract dispensaries
                citiesProcessed++;
                const location = parseLocationFromUrl(url);
                
                log.info(`Processing city page: ${location.citySlug} (${location.state}, ${location.country})`);

                // Wait for dispensary cards to load
                try {
                    await page.waitForSelector('a[href*="/dispensary/"]', { timeout: 15000 });
                } catch (e) {
                    log.warning(`No dispensaries found on ${url}`);
                    return;
                }

                // Scroll to load all dispensaries (infinite scroll handling)
                let previousHeight = 0;
                let scrollAttempts = 0;
                const maxScrollAttempts = 20;

                while (scrollAttempts < maxScrollAttempts) {
                    const currentHeight = await page.evaluate(() => document.body.scrollHeight);
                    if (currentHeight === previousHeight) break;
                    
                    previousHeight = currentHeight;
                    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
                    await page.waitForTimeout(1500);
                    scrollAttempts++;
                }

                // Extract dispensary data
                const dispensaries = await page.evaluate(() => {
                    const results = [];
                    const links = document.querySelectorAll('a[href*="/dispensary/"]');
                    
                    links.forEach(link => {
                        const href = link.getAttribute('href');
                        if (!href) return;
                        
                        const slug = href.replace('/dispensary/', '').split('?')[0];
                        const rawText = link.textContent || '';
                        
                        // Only add if we haven't seen this slug in current extraction
                        if (slug && !results.find(r => r.slug === slug)) {
                            results.push({
                                slug,
                                rawName: rawText
                            });
                        }
                    });
                    
                    return results;
                });

                log.info(`Found ${dispensaries.length} dispensaries on ${location.citySlug}`);

                // Process and deduplicate dispensaries
                for (const disp of dispensaries) {
                    if (!dispensaryMap.has(disp.slug)) {
                        const cleanName = extractDispensaryName(disp.rawName);
                        
                        dispensaryMap.set(disp.slug, {
                            cName: disp.slug,
                            name: cleanName,
                            city_page: url,
                            state: location.state,
                            country: location.country
                        });
                    }
                }

                log.info(`Total unique dispensaries so far: ${dispensaryMap.size}`);
            }

            // Rate limiting delay
            if (delayBetweenRequests > 0) {
                await page.waitForTimeout(delayBetweenRequests);
            }
        },

        // Handle failures
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Failed to process ${request.url}: ${error.message}`);
            citiesFailed++;
        }
    });

    // Start with custom URLs or the cities page
    if (startUrls && startUrls.length > 0) {
        log.info(`Starting with ${startUrls.length} custom URLs`);
        for (const urlItem of startUrls) {
            const url = typeof urlItem === 'string' ? urlItem : urlItem.url;
            await requestQueue.addRequest({ url });
        }
    } else {
        log.info('Starting with cities index page');
        await requestQueue.addRequest({ url: CITIES_URL });
    }

    // Run the crawler
    await crawler.run();

    // Prepare final output
    log.info('Scraping completed. Preparing output...');
    
    // Convert Map to object keyed by slug
    const outputObject = {};
    dispensaryMap.forEach((value, key) => {
        outputObject[key] = value;
    });

    // Save to Apify Dataset (JSON)
    const dataset = await Actor.openDataset();
    
    // Save individual items for dataset
    const dispensaryArray = Array.from(dispensaryMap.values());
    for (const disp of dispensaryArray) {
        await dataset.pushData(disp);
    }

    // Save the keyed JSON to key-value store
    const kvStore = await Actor.openKeyValueStore();
    await kvStore.setValue('dispensaries_by_slug', outputObject);

    // Generate CSV content
    const csvHeaders = ['cName', 'name', 'city_page', 'state', 'country'];
    const csvRows = dispensaryArray.map(d => 
        csvHeaders.map(h => `"${(d[h] || '').replace(/"/g, '""')}"`).join(',')
    );
    const csvContent = [csvHeaders.join(','), ...csvRows].join('\n');
    
    await kvStore.setValue('dispensaries', csvContent, { contentType: 'text/csv' });

    // Log summary
    const summary = {
        totalDispensaries: dispensaryMap.size,
        citiesProcessed,
        citiesFailed,
        countries: [...new Set(dispensaryArray.map(d => d.country))],
        stateCount: [...new Set(dispensaryArray.map(d => d.state))].length
    };

    log.info('=== SCRAPING SUMMARY ===');
    log.info(`Total unique dispensaries: ${summary.totalDispensaries}`);
    log.info(`Cities processed: ${summary.citiesProcessed}`);
    log.info(`Cities failed: ${summary.citiesFailed}`);
    log.info(`Countries: ${summary.countries.join(', ')}`);
    log.info(`Number of states/provinces: ${summary.stateCount}`);

    await kvStore.setValue('summary', summary);

    log.info('All data saved to dataset and key-value store');
});
