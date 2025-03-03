const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const axios = require('axios');
const Airtable = require('airtable');
const yaml = require('js-yaml');
const fs = require('fs');
const { google } = require('googleapis');
const { logger, setLogLevel } = require('./logger');

// Set logging level from environment variable or use default from logger
if (process.env.LOG_LEVEL) {
  setLogLevel(process.env.LOG_LEVEL);
}

// Global variables to cache expensive operations
let configCache = null;
let googleAuthClient = null;
let sheetsApiClient = null;
const regexCache = new Map();
const airtableBaseCache = new Map();
const sheetsHeadersCache = new Map();  // Cache for sheet headers

// Function to determine if initialization is complete
function isInitializationComplete() {
  // Check if we have an auth client for Google Sheets
  const hasGoogleAuth = !!googleAuthClient;

  // Check if config is cached
  const hasConfig = !!configCache;

  // If we have sheets configured, check if headers are prefetched
  let headersReady = true;
  if (configCache && configCache.googlesheets) {
    const totalSheets = configCache.googlesheets.length;
    const cachedSheets = sheetsHeadersCache.size;

    if (totalSheets > 0 && cachedSheets < totalSheets) {
      headersReady = false;
    }
  }

  return hasConfig && (
    // Either we have no Google Sheets or we have auth and headers
    !configCache?.googlesheets?.length ||
    (hasGoogleAuth && headersReady)
  );
}

// Get initialization status message
function getInitializationStatus() {
  const status = {
    configLoaded: !!configCache,
    googleAuthInitialized: !!googleAuthClient,
    totalSheets: configCache?.googlesheets?.length || 0,
    sheetHeadersPrefetched: sheetsHeadersCache.size,
    isComplete: isInitializationComplete()
  };

  return status;
}

// Initialize function - called once on cold start
async function initialize() {
  logger.info('Initializing webhook handler...');
  const initStart = Date.now();

  try {
    // Pre-load configuration
    const config = loadConfig();

    // Pre-initialize Google auth
    if (config.googlesheets && config.googlesheets.length > 0) {
      await initGoogleAuth();

      // Prefetch all Google Sheets headers
      if (sheetsApiClient) {
        logger.info(`Prefetching headers for ${config.googlesheets.length} Google Sheets...`);
        const prefetchStart = Date.now();

        // Use Promise.all to prefetch headers in parallel
        await Promise.all(
          config.googlesheets.map(async (sheet) => {
            try {
              const headersCacheKey = `${sheet.spreadsheetId}_${sheet.sheetName}`;

              // Skip if already cached
              if (sheetsHeadersCache.has(headersCacheKey)) {
                return;
              }

              logger.debug(`Prefetching headers for sheet: ${sheet.sheetName}`);
              const sheetStart = Date.now();

              const headerResponse = await sheetsApiClient.spreadsheets.values.get({
                spreadsheetId: sheet.spreadsheetId,
                range: `${sheet.sheetName}!1:1`,
              });

              if (headerResponse.data.values && headerResponse.data.values[0]) {
                sheetsHeadersCache.set(headersCacheKey, headerResponse.data.values[0]);
                logger.timing(`Prefetched headers for ${sheet.sheetName} in ${Date.now() - sheetStart}ms`);
              } else {
                logger.error(`Failed to prefetch headers for ${sheet.sheetName}: No data returned`);
              }
            } catch (error) {
              logger.error(`Error prefetching headers for sheet ${sheet.spreadsheetId}/${sheet.sheetName}:`, error);
              // We'll continue with other sheets and handle this one at runtime if needed
            }
          })
        );

        logger.timing(`Completed prefetching headers in ${Date.now() - prefetchStart}ms`);
      }
    }

    logger.timing(`Initialization completed in ${Date.now() - initStart}ms`);
  } catch (error) {
    logger.error('Error during initialization:', error);
    logger.warn('Continuing with partial initialization');
  }
}

// Load and cache configuration from YAML file
function loadConfig() {
  if (configCache) {
    return configCache;
  }

  try {
    const configFile = process.env.CONFIG_FILE_PATH || './config.yaml';
    logger.info(`Loading configuration from: ${configFile}`);
    const fileContents = fs.readFileSync(configFile, 'utf8');
    const config = yaml.load(fileContents);

    // Safety checks
    if (!config) {
      logger.error('Configuration file is empty or invalid');
      configCache = { bases: [], googlesheets: [] };
      return configCache;
    }

    if (!config.bases) config.bases = [];
    if (!config.googlesheets) config.googlesheets = [];

    // Pre-compile all regex patterns for faster matching
    config.bases.forEach(base => {
      if (base.condition) {
        try {
          const regex = new RegExp(base.condition, 'i');
          regexCache.set(`base_${base.baseId}_${base.condition}`, regex);
        } catch (e) {
          logger.error(`Invalid regex in base ${base.baseId} condition: ${base.condition}`, e);
        }
      }
      // Set default priority
      base.priority = base.priority || 100;
    });

    config.googlesheets.forEach(sheet => {
      if (sheet.condition) {
        try {
          const regex = new RegExp(sheet.condition, 'i');
          regexCache.set(`sheet_${sheet.spreadsheetId}_${sheet.condition}`, regex);
        } catch (e) {
          logger.error(`Invalid regex in sheet ${sheet.spreadsheetId} condition: ${sheet.condition}`, e);
        }
      }
      // Set default priority and type
      sheet.priority = sheet.priority || 100;
      sheet.type = 'googlesheet';
    });

    logger.info(`Loaded ${config.bases.length} Airtable bases and ${config.googlesheets.length} Google Sheets configurations`);
    configCache = config;
    return configCache;
  } catch (e) {
    logger.error('Error loading configuration:', e);
    configCache = { bases: [], googlesheets: [] };
    return configCache;
  }
}

// Pre-initialize Google auth client
async function initGoogleAuth() {
  if (googleAuthClient) {
    return googleAuthClient;
  }

  try {
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      // If using a credentials file path
      logger.debug('Using GOOGLE_APPLICATION_CREDENTIALS for authentication');
      googleAuthClient = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
      });
    } else if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
      // If credentials are provided as a JSON string in environment
      logger.debug('Using GOOGLE_SERVICE_ACCOUNT_KEY for authentication');
      const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
      googleAuthClient = new google.auth.JWT(
        credentials.client_email,
        null,
        credentials.private_key,
        ['https://www.googleapis.com/auth/spreadsheets']
      );
      await googleAuthClient.authorize();
    } else if (process.env.FUNCTION_IDENTITY) {
      // If running on Google Cloud with proper IAM permissions
      logger.debug('Using FUNCTION_IDENTITY for authentication');
      googleAuthClient = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
      });
    } else {
      throw new Error('No Google authentication method available. Please set up credentials.');
    }

    // Initialize Sheets API client
    sheetsApiClient = google.sheets({ version: 'v4', auth: googleAuthClient });

    logger.info('Google auth initialized successfully');
    return googleAuthClient;
  } catch (error) {
    logger.error('Error initializing Google auth:', error);
    throw error;
  }
}

// Get cached Airtable base instance
function getAirtableBase(apiKey, baseId) {
  const cacheKey = `${apiKey}_${baseId}`;

  if (!airtableBaseCache.has(cacheKey)) {
    const base = new Airtable({ apiKey }).base(baseId);
    airtableBaseCache.set(cacheKey, base);
    return base;
  }

  return airtableBaseCache.get(cacheKey);
}

// Helper function to apply field mappings to a record
function applyFieldMappings(record, fieldMappings) {
  if (!fieldMappings) return { ...record };

  return Object.keys(record).reduce((mapped, key) => {
    const mappedKey = fieldMappings[key] || key;
    mapped[mappedKey] = record[key];
    return mapped;
  }, {});
}

// Generic retry function with exponential backoff
async function withRetry(operation, name, maxAttempts = 5, baseDelayMs = 1000) {
  let lastError;
  const startTime = Date.now();

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const opStart = Date.now();
      const result = await operation();

      if (attempt > 1) {
        // Only log timing for retry successes (attempts after the first)
        logger.timing(`Operation '${name}' succeeded on attempt ${attempt} after ${Date.now() - opStart}ms (total time with retries: ${Date.now() - startTime}ms)`);
      }

      return result;
    } catch (error) {
      lastError = error;

      // Check if it's a rate limiting error (429)
      const isRateLimitError = error.statusCode === 429 ||
        (error.code === 429) ||
        (error.message && error.message.includes('quota'));

      if (isRateLimitError && attempt < maxAttempts) {
        const delay = Math.min(baseDelayMs * Math.pow(2, attempt - 1), 30000); // Cap at 30 seconds
        logger.warn(`Rate limit hit for '${name}' after ${Date.now() - opStart}ms. Retrying in ${delay}ms (attempt ${attempt}/${maxAttempts})`);

        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      logger.error(`Error in '${name}' (attempt ${attempt}/${maxAttempts}) after ${Date.now() - opStart}ms:`, error);
      throw error;
    }
  }

  throw lastError;
}

// Function to save record to a specific Airtable base and table with retry logic
async function saveToAirtable(record, baseConfig, apiKey) {
  const startTime = Date.now();

  try {
    const result = await withRetry(async () => {
      const base = getAirtableBase(apiKey, baseConfig.baseId);
      const table = base(baseConfig.tableId);

      // Apply field mappings if specified
      const mappedRecord = applyFieldMappings(record, baseConfig.fieldMappings);

      // Save the record to Airtable
      return new Promise((resolve, reject) => {
        table.create(
          [{ fields: mappedRecord }],
          function (err, records) {
            if (err) {
              reject(err);
              return;
            }

            records.forEach(function (record) {
              logger.info(`Record saved to Airtable base ${baseConfig.baseId}, table ${baseConfig.tableId}: ${record.getId()}`);
            });
            resolve(records);
          }
        );
      });
    }, `Airtable ${baseConfig.baseId}/${baseConfig.tableId}`);

    const duration = Date.now() - startTime;
    // We'll only log timing at the destination level
    return result;
  } catch (error) {
    const duration = Date.now() - startTime;
    // Error logging remains at this level for debugging
    logger.error(`Internal Airtable error for ${baseConfig.baseId}/${baseConfig.tableId} after ${duration}ms:`, error);
    throw error;
  }
}

// Function to save record to a specific Google Sheet
async function saveToGoogleSheet(record, sheetConfig) {
  const startTime = Date.now();

  try {
    // Ensure Google auth is initialized
    if (!sheetsApiClient) {
      const authStart = Date.now();
      await initGoogleAuth();
      logger.timing(`Google auth initialization took ${Date.now() - authStart}ms`);
    }

    const result = await withRetry(async () => {
      // Apply field mappings if specified
      const mappedRecord = applyFieldMappings(record, sheetConfig.fieldMappings);

      // Get headers from cache
      const headersCacheKey = `${sheetConfig.spreadsheetId}_${sheetConfig.sheetName}`;
      let headers = sheetsHeadersCache.get(headersCacheKey);

      // If not in cache, fetch them now
      if (!headers) {
        const headersStart = Date.now();
        logger.warn(`Headers not prefetched for ${sheetConfig.sheetName}, fetching now`);

        // Get the column headers from the sheet
        const headerResponse = await sheetsApiClient.spreadsheets.values.get({
          spreadsheetId: sheetConfig.spreadsheetId,
          range: `${sheetConfig.sheetName}!1:1`,
        });

        if (!headerResponse.data.values || !headerResponse.data.values[0]) {
          throw new Error(`No headers found in sheet ${sheetConfig.sheetName}`);
        }

        headers = headerResponse.data.values[0];
        sheetsHeadersCache.set(headersCacheKey, headers);
        logger.timing(`Headers fetch for ${sheetConfig.sheetName}: ${Date.now() - headersStart}ms`);
      }

      // Create an ordered row based on the headers
      const rowData = headers.map(header => mappedRecord[header] || '');

      // Append the data
      const appendStart = Date.now();

      // Use the append method for better performance
      const appendResponse = await sheetsApiClient.spreadsheets.values.append({
        spreadsheetId: sheetConfig.spreadsheetId,
        range: `${sheetConfig.sheetName}!A1`,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: {
          values: [rowData],
        },
      });

      logger.timing(`Sheet append for ${sheetConfig.sheetName}: ${Date.now() - appendStart}ms`);

      // Extract information about where the data was appended
      const updatedRange = appendResponse.data.updates.updatedRange;
      logger.info(`Record appended to Google Sheet ${sheetConfig.spreadsheetId}, sheet ${sheetConfig.sheetName}, range: ${updatedRange}`);

      return { sheet: sheetConfig.sheetName, range: updatedRange };
    }, `GoogleSheet ${sheetConfig.spreadsheetId}/${sheetConfig.sheetName}`);

    const duration = Date.now() - startTime;
    return result;
  } catch (error) {
    const duration = Date.now() - startTime;
    logger.error(`Internal Google Sheets error for ${sheetConfig.spreadsheetId}/${sheetConfig.sheetName} after ${duration}ms:`, error);
    throw error;
  }
}

// Function to map webhook data to Airtable record
function mapWebhookDataToAirtableRecord(webhookData) {
  const eventType = webhookData.event;
  const payload = webhookData.payload;
  const object = payload.object;
  const participant = object.participant;

  // Base record with common fields
  const record = {
    "Event Type": eventType,
    "Event Timestamp": webhookData.event_ts,
    "Account ID": payload.account_id,
    "Meeting ID": object.id,
    "Meeting UUID": object.uuid,
    "Host ID": object.host_id,
    "Meeting Topic": object.topic,
    "Meeting Type": object.type,
    "Start Time": object.start_time,
    "Timezone": object.timezone,
    "Duration": object.duration,
    "Participant User ID": participant.user_id,
    "Participant Name": participant.user_name,
    "Participant ID": participant.id,
    "Participant UUID": participant.participant_uuid,
    "Email": participant.email,
    "Registrant ID": participant.registrant_id,
    "Participant User ID Alt": participant.participant_user_id,
    "Customer Key": participant.customer_key,
    "Phone Number": participant.phone_number
  };

  // Handle event-specific fields
  if (eventType === "webinar.participant_joined") {
    record["Event Datetime"] = participant.join_time;
  } else if (eventType === "webinar.participant_left") {
    record["Leave Time"] = participant.leave_time;
    record["Leave Reason"] = participant.leave_reason;
    record["Event Datetime"] = participant.leave_time;
  } else if (eventType === "meeting.participant_joined") {
    record["Event Datetime"] = participant.date_time;
  } else if (eventType === "meeting.participant_left") {
    record["Leave Time"] = participant.leave_time;
    record["Leave Reason"] = participant.leave_reason;
    record["Event Datetime"] = participant.date_time;
  }

  return record;
}

// Function to determine which destinations to use based on meeting topic
function determineTargetBases(meetingTopic, config) {
  const matchingDestinations = [];
  const logPrefix = `[Topic Match: ${meetingTopic}]`;

  // Process Airtable bases
  if (config.bases) {
    for (const base of config.bases) {
      let matches = false;

      // If the base has no condition, consider it a match
      if (!base.condition) {
        matches = true;
        logger.debug(`${logPrefix} Base ${base.baseId} matched (no condition)`);
      } else {
        // Try to match the condition using cached regex
        try {
          const cacheKey = `base_${base.baseId}_${base.condition}`;
          let regex = regexCache.get(cacheKey);

          if (!regex) {
            regex = new RegExp(base.condition, 'i');
            regexCache.set(cacheKey, regex);
          }

          matches = regex.test(meetingTopic);

          if (matches) {
            logger.debug(`${logPrefix} Base ${base.baseId} condition "${base.condition}" matched`);
          }
        } catch (error) {
          logger.error(`Invalid regex in base ${base.baseId} condition: ${base.condition}`, error);
          continue;
        }
      }

      if (matches) {
        matchingDestinations.push(base);
      }
    }
  }

  // Process Google Sheets
  if (config.googlesheets) {
    for (const sheet of config.googlesheets) {
      let matches = false;

      // If the sheet has no condition, consider it a match
      if (!sheet.condition) {
        matches = true;
        logger.debug(`${logPrefix} Sheet ${sheet.spreadsheetId}/${sheet.sheetName} matched (no condition)`);
      } else {
        // Try to match the condition using cached regex
        try {
          const cacheKey = `sheet_${sheet.spreadsheetId}_${sheet.condition}`;
          let regex = regexCache.get(cacheKey);

          if (!regex) {
            regex = new RegExp(sheet.condition, 'i');
            regexCache.set(cacheKey, regex);
          }

          matches = regex.test(meetingTopic);

          if (matches) {
            logger.debug(`${logPrefix} Sheet ${sheet.spreadsheetId}/${sheet.sheetName} condition "${sheet.condition}" matched`);
          }
        } catch (error) {
          logger.error(`Invalid regex in sheet ${sheet.spreadsheetId}/${sheet.sheetName} condition: ${sheet.condition}`, error);
          continue;
        }
      }

      if (matches) {
        matchingDestinations.push(sheet);
      }
    }
  }

  // Sort destinations by priority (lower numbers first)
  const sortedDestinations = matchingDestinations.sort((a, b) => a.priority - b.priority);

  if (sortedDestinations.length > 0) {
    logger.info(`${logPrefix} Found ${sortedDestinations.length} matching destinations`);
  } else {
    logger.info(`${logPrefix} No matching destinations found`);
  }

  return sortedDestinations;
}

// Initialize the Express app
const app = express();
app.use(bodyParser.json());

// Run initialization asynchronously on module load
(async function () {
  try {
    await initialize();
  } catch (error) {
    logger.error('Error during async initialization:', error);
  }
})();

app.post('/', async (req, res) => {
  const startTime = Date.now();
  let response;

  try {
    logger.timing(`Webhook request received at ${new Date().toISOString()}`);

    // Check initialization status
    const initStatus = getInitializationStatus();
    if (!initStatus.isComplete) {
      logger.warn(`Processing webhook while initialization still in progress:`, initStatus);
    }

    // Verify Zoom webhook signature
    const verifyStart = Date.now();
    const message = `v0:${req.headers['x-zm-request-timestamp']}:${JSON.stringify(req.body)}`;
    const hashForVerify = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN).update(message).digest('hex');
    const signature = `v0=${hashForVerify}`;
    logger.debug(`Signature verification completed in ${Date.now() - verifyStart}ms`);

    // Check if request is authentic
    if (req.headers['x-zm-signature'] !== signature) {
      response = { message: 'Unauthorized request to Zoom Webhook Catcher.', status: 401 };
      logger.warn(response.message);
      res.status(response.status);
      res.json(response);
      logger.timing(`Unauthorized request rejected in ${Date.now() - startTime}ms`);
      return;
    }

    // Handle Zoom validation challenge
    if (req.body.event === 'endpoint.url_validation') {
      const validationStart = Date.now();
      const hashForValidate = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN)
        .update(req.body.payload.plainToken).digest('hex');

      response = {
        message: {
          plainToken: req.body.payload.plainToken,
          encryptedToken: hashForValidate
        },
        status: 200
      };

      logger.timing(`Webhook validation response prepared in ${Date.now() - validationStart}ms`);
      res.status(response.status);
      res.json(response.message);
      logger.timing(`Webhook validation completed in ${Date.now() - startTime}ms`);
      return;
    }

    // For regular webhooks, send response immediately to avoid timeouts
    response = { message: 'Authorized request to Zoom Webhook Catcher.', status: 200 };
    res.status(response.status);
    res.json(response);
    logger.timing(`Initial response sent in ${Date.now() - startTime}ms for event: ${req.body.event}`);

    // Process the webhook asynchronously (after responding to Zoom)
    processWebhook(req.body, req.headers)
      .then(() => {
        logger.timing(`Webhook processing completed in ${Date.now() - startTime}ms`);
      })
      .catch(error => {
        logger.error(`Error in async webhook processing after ${Date.now() - startTime}ms:`, error);
      });

  } catch (error) {
    logger.error(`Error in webhook handler after ${Date.now() - startTime}ms:`, error);

    // If we haven't sent a response yet, send a 500
    if (!res.headersSent) {
      res.status(500).json({ message: 'Internal server error' });
      logger.timing(`Error response sent in ${Date.now() - startTime}ms`);
    }
  }
});

// Async function to process webhook data after responding to Zoom
async function processWebhook(body, headers) {
  const processingStart = Date.now();
  logger.timing(`Starting webhook processing for event: ${body.event}`);

  try {
    // Forward the webhook if configured
    if (process.env.FORWARD_WEBHOOK_URL) {
      const forwardStart = Date.now();
      try {
        await axios.post(process.env.FORWARD_WEBHOOK_URL, body, {
          headers: { 'Content-Type': 'application/json' }
        });
        logger.timing(`Webhook forwarding completed in ${Date.now() - forwardStart}ms`);
      } catch (error) {
        logger.error(`Webhook forwarding failed after ${Date.now() - forwardStart}ms:`, error);
      }
    }

    // Check if this is a relevant event to process
    const relevantEvents = [
      'webinar.participant_joined',
      'webinar.participant_left',
      'meeting.participant_joined',
      'meeting.participant_left'
    ];

    if (!relevantEvents.includes(body.event)) {
      logger.debug(`Skipping event type: ${body.event} (not a relevant event)`);
      return;
    }

    // Process the event data
    const mapStart = Date.now();
    const record = mapWebhookDataToAirtableRecord(body);
    logger.timing(`Record mapping completed in ${Date.now() - mapStart}ms`);

    const meetingTopic = body.payload.object.topic || '';
    logger.info(`Processing webhook for meeting topic: "${meetingTopic}"`);

    // Get matching destinations
    const routingStart = Date.now();
    const config = loadConfig();
    const targetBases = determineTargetBases(meetingTopic, config);
    logger.timing(`Destination routing completed in ${Date.now() - routingStart}ms`);

    if (targetBases.length === 0) {
      logger.info(`No matching destinations found for topic: "${meetingTopic}"`);

      // Fallback to the environment variable settings if configured
      if (process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID && process.env.AIRTABLE_TABLE_ID) {
        const fallbackBase = {
          baseId: process.env.AIRTABLE_BASE_ID,
          tableId: process.env.AIRTABLE_TABLE_ID
        };
        logger.info('Using fallback Airtable configuration from environment variables');
        await saveToAirtable(record, fallbackBase, process.env.AIRTABLE_API_KEY);
      }
      return;
    }

    // Save to all matching destinations with rate limiting protection
    const apiKey = process.env.AIRTABLE_API_KEY;

    const saveStart = Date.now();
    logger.info(`Processing ${targetBases.length} destinations`);

    // Use Promise.all with a concurrency limit for better performance
    // This allows some parallelism while avoiding overwhelming the APIs
    await processBatchesWithConcurrency(targetBases, async (destination) => {
      const destStart = Date.now();
      const destType = destination.type === 'googlesheet' ? 'GoogleSheet' : 'Airtable';
      const destId = destination.type === 'googlesheet'
        ? `${destination.spreadsheetId}/${destination.sheetName}`
        : `${destination.baseId}/${destination.tableId}`;

      logger.debug(`Processing ${destType}: ${destId}`);

      try {
        if (destination.type === 'googlesheet') {
          await saveToGoogleSheet(record, destination);
        } else {
          await saveToAirtable(record, destination, apiKey);
        }

        logger.timing(`Completed ${destType}: ${destId} in ${Date.now() - destStart}ms`);
      } catch (error) {
        logger.error(`Failed ${destType}: ${destId} after ${Date.now() - destStart}ms`, error);
      }
    }, 3); // Process up to 3 destinations at once

    logger.timing(`All destinations processed in ${Date.now() - saveStart}ms`);
    logger.timing(`Total webhook processing time: ${Date.now() - processingStart}ms`);

  } catch (error) {
    logger.error(`Error in webhook processing after ${Date.now() - processingStart}ms:`, error);
    throw error;
  }
}

// Helper function to process items with limited concurrency
async function processBatchesWithConcurrency(items, processor, concurrency = 3) {
  const results = [];
  const batches = [];

  // Split items into batches based on concurrency
  for (let i = 0; i < items.length; i += concurrency) {
    batches.push(items.slice(i, i + concurrency));
  }

  // Process each batch sequentially, but items within a batch in parallel
  for (const batch of batches) {
    const batchResults = await Promise.all(
      batch.map(item => processor(item))
    );
    results.push(...batchResults);
  }

  return results;
}

functions.http('zoomWebhook', app);