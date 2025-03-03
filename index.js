const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const axios = require('axios');
const Airtable = require('airtable');
const yaml = require('js-yaml');
const fs = require('fs');
const {google} = require('googleapis');

// Global variables to cache expensive operations
let configCache = null;
let googleAuthClient = null;
let sheetsApiClient = null;
const regexCache = new Map();
const airtableBaseCache = new Map();

// Initialize function - called once on cold start
function initialize() {
  console.log('Initializing webhook handler...');
  
  // Pre-load configuration
  loadConfig();
  
  // Pre-initialize Google auth
  initGoogleAuth().catch(err => {
    console.error('Failed to initialize Google auth:', err);
  });
  
  console.log('Initialization complete');
}

// Load and cache configuration from YAML file
function loadConfig() {
  if (configCache) {
    return configCache;
  }
  
  try {
    const configFile = process.env.CONFIG_FILE_PATH || './config.yaml';
    console.log(`Loading configuration from: ${configFile}`);
    const fileContents = fs.readFileSync(configFile, 'utf8');
    const config = yaml.load(fileContents);
    
    // Safety checks
    if (!config) {
      console.error('Configuration file is empty or invalid');
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
          console.error(`Invalid regex in base ${base.baseId} condition: ${base.condition}`, e);
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
          console.error(`Invalid regex in sheet ${sheet.spreadsheetId} condition: ${sheet.condition}`, e);
        }
      }
      // Set default priority and type
      sheet.priority = sheet.priority || 100;
      sheet.type = 'googlesheet';
    });
    
    console.log(`Loaded ${config.bases.length} Airtable bases and ${config.googlesheets.length} Google Sheets configurations`);
    configCache = config;
    return configCache;
  } catch (e) {
    console.error('Error loading configuration:', e);
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
      googleAuthClient = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
      });
    } else if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
      // If credentials are provided as a JSON string in environment
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
      googleAuthClient = new google.auth.GoogleAuth({
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
      });
    } else {
      throw new Error('No Google authentication method available. Please set up credentials.');
    }
    
    // Initialize Sheets API client
    sheetsApiClient = google.sheets({ version: 'v4', auth: googleAuthClient });
    
    console.log('Google auth initialized successfully');
    return googleAuthClient;
  } catch (error) {
    console.error('Error initializing Google auth:', error);
    throw error;
  }
}

// Get cached Airtable base instance
function getAirtableBase(apiKey, baseId) {
  const cacheKey = `${apiKey}_${baseId}`;
  
  if (!airtableBaseCache.has(cacheKey)) {
    const base = new Airtable({apiKey}).base(baseId);
    airtableBaseCache.set(cacheKey, base);
    return base;
  }
  
  return airtableBaseCache.get(cacheKey);
}

// Helper function to apply field mappings to a record
function applyFieldMappings(record, fieldMappings) {
  if (!fieldMappings) return {...record};
  
  return Object.keys(record).reduce((mapped, key) => {
    const mappedKey = fieldMappings[key] || key;
    mapped[mappedKey] = record[key];
    return mapped;
  }, {});
}

// Generic retry function with exponential backoff
async function withRetry(operation, name, maxAttempts = 5, baseDelayMs = 1000) {
  let lastError;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      
      // Check if it's a rate limiting error (429)
      const isRateLimitError = error.statusCode === 429 || 
                              (error.code === 429) || 
                              (error.message && error.message.includes('quota'));
      
      if (isRateLimitError && attempt < maxAttempts) {
        const delay = Math.min(baseDelayMs * Math.pow(2, attempt - 1), 30000); // Cap at 30 seconds
        console.log(`Rate limit hit for ${name}. Retrying in ${delay}ms (attempt ${attempt}/${maxAttempts})`);
        
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      
      console.error(`Error in ${name} (attempt ${attempt}/${maxAttempts}):`, error);
      throw error;
    }
  }
  
  throw lastError;
}

// Function to save record to a specific Airtable base and table with retry logic
async function saveToAirtable(record, baseConfig, apiKey) {
  return withRetry(async () => {
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
            console.log(`Record saved to Airtable base ${baseConfig.baseId}, table ${baseConfig.tableId}: ${record.getId()}`);
          });
          resolve(records);
        }
      );
    });
  }, `Airtable base ${baseConfig.baseId}`);
}

// Function to save record to a specific Google Sheet
async function saveToGoogleSheet(record, sheetConfig) {
  // Ensure Google auth is initialized
  if (!sheetsApiClient) {
    await initGoogleAuth();
  }
  
  return withRetry(async () => {
    // Apply field mappings if specified
    const mappedRecord = applyFieldMappings(record, sheetConfig.fieldMappings);
    
    // Use caching to optimize header fetching
    const headersCacheKey = `headers_${sheetConfig.spreadsheetId}_${sheetConfig.sheetName}`;
    let headers = regexCache.get(headersCacheKey);
    
    if (!headers) {
      // First, get the column headers from the sheet to ensure proper ordering
      const headerResponse = await sheetsApiClient.spreadsheets.values.get({
        spreadsheetId: sheetConfig.spreadsheetId,
        range: `${sheetConfig.sheetName}!1:1`,
      });
      
      headers = headerResponse.data.values[0];
      regexCache.set(headersCacheKey, headers);
    }
    
    // Create an ordered row based on the headers
    const rowData = headers.map(header => mappedRecord[header] || '');
    
    // Instead of querying the entire sheet, use the append method
    // This is much faster as it automatically appends to the end of data
    const appendResponse = await sheetsApiClient.spreadsheets.values.append({
      spreadsheetId: sheetConfig.spreadsheetId,
      range: `${sheetConfig.sheetName}!A1`,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      resource: {
        values: [rowData],
      },
    });
    
    // Extract information about where the data was appended
    const updatedRange = appendResponse.data.updates.updatedRange;
    console.log(`Record appended to Google Sheet ${sheetConfig.spreadsheetId}, sheet ${sheetConfig.sheetName}, range: ${updatedRange}`);
    
    return { sheet: sheetConfig.sheetName, range: updatedRange };
  }, `Google Sheet ${sheetConfig.spreadsheetId}`);
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
        console.log(`${logPrefix} Base ${base.baseId} matched (no condition)`);
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
            console.log(`${logPrefix} Base ${base.baseId} condition "${base.condition}" matched`);
          }
        } catch (error) {
          console.error(`Invalid regex in base ${base.baseId} condition: ${base.condition}`, error);
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
        console.log(`${logPrefix} Sheet ${sheet.spreadsheetId}/${sheet.sheetName} matched (no condition)`);
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
            console.log(`${logPrefix} Sheet ${sheet.spreadsheetId}/${sheet.sheetName} condition "${sheet.condition}" matched`);
          }
        } catch (error) {
          console.error(`Invalid regex in sheet ${sheet.spreadsheetId}/${sheet.sheetName} condition: ${sheet.condition}`, error);
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
    console.log(`${logPrefix} Found ${sortedDestinations.length} matching destinations`);
  } else {
    console.log(`${logPrefix} No matching destinations found`);
  }
  
  return sortedDestinations;
}

// Initialize the Express app
const app = express();
app.use(bodyParser.json());

// Run initialization on module load
initialize();

app.post('/', async (req, res) => {
  const startTime = Date.now();
  let response;

  try {
    // Verify Zoom webhook signature
    const message = `v0:${req.headers['x-zm-request-timestamp']}:${JSON.stringify(req.body)}`;
    const hashForVerify = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN).update(message).digest('hex');
    const signature = `v0=${hashForVerify}`;

    // Check if request is authentic
    if (req.headers['x-zm-signature'] !== signature) {
      response = { message: 'Unauthorized request to Zoom Webhook Catcher.', status: 401 };
      console.log(response.message);
      res.status(response.status);
      res.json(response);
      return;
    }

    // Handle Zoom validation challenge
    if (req.body.event === 'endpoint.url_validation') {
      const hashForValidate = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN)
        .update(req.body.payload.plainToken).digest('hex');

      response = {
        message: {
          plainToken: req.body.payload.plainToken,
          encryptedToken: hashForValidate
        },
        status: 200
      };

      console.log('Webhook validation response sent');
      res.status(response.status);
      res.json(response.message);
      return;
    }

    // For regular webhooks, send response immediately to avoid timeouts
    response = { message: 'Authorized request to Zoom Webhook Catcher.', status: 200 };
    res.status(response.status);
    res.json(response);
    console.log(`Webhook received: ${req.body.event}, responded in ${Date.now() - startTime}ms`);

    // Process the webhook asynchronously (after responding to Zoom)
    processWebhook(req.body, req.headers)
      .then(() => {
        console.log(`Webhook processing completed in ${Date.now() - startTime}ms`);
      })
      .catch(error => {
        console.error('Error in async webhook processing:', error);
      });

  } catch (error) {
    console.error('Error in webhook handler:', error);
    
    // If we haven't sent a response yet, send a 500
    if (!res.headersSent) {
      res.status(500).json({ message: 'Internal server error' });
    }
  }
});

// Async function to process webhook data after responding to Zoom
async function processWebhook(body, headers) {
  try {
    // Forward the webhook if configured
    if (process.env.FORWARD_WEBHOOK_URL) {
      try {
        await axios.post(process.env.FORWARD_WEBHOOK_URL, body, {
          headers: { 'Content-Type': 'application/json' }
        });
        console.log('Webhook forwarded successfully');
      } catch (error) {
        console.error('Error forwarding webhook:', error);
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
      console.log(`Skipping event type: ${body.event} (not a relevant event)`);
      return;
    }
    
    // Process the event data
    const record = mapWebhookDataToAirtableRecord(body);
    const meetingTopic = body.payload.object.topic || '';
    console.log(`Processing webhook for meeting topic: "${meetingTopic}"`);
    
    // Get matching destinations
    const config = loadConfig();
    const targetBases = determineTargetBases(meetingTopic, config);
    
    if (targetBases.length === 0) {
      console.log(`No matching destinations found for topic: "${meetingTopic}"`);
      
      // Fallback to the environment variable settings if configured
      if (process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID && process.env.AIRTABLE_TABLE_ID) {
        const fallbackBase = {
          baseId: process.env.AIRTABLE_BASE_ID,
          tableId: process.env.AIRTABLE_TABLE_ID
        };
        console.log('Using fallback Airtable configuration from environment variables');
        await saveToAirtable(record, fallbackBase, process.env.AIRTABLE_API_KEY);
      }
      return;
    }
    
    // Save to all matching destinations with rate limiting protection
    const apiKey = process.env.AIRTABLE_API_KEY;
    
    // Use Promise.all with a concurrency limit for better performance
    // This allows some parallelism while avoiding overwhelming the APIs
    await processBatchesWithConcurrency(targetBases, async (destination) => {
      try {
        if (destination.type === 'googlesheet') {
          await saveToGoogleSheet(record, destination);
          console.log(`Webhook data saved to Google Sheet ${destination.spreadsheetId}/${destination.sheetName}`);
        } else {
          await saveToAirtable(record, destination, apiKey);
          console.log(`Webhook data saved to Airtable base ${destination.baseId}/${destination.tableId}`);
        }
      } catch (error) {
        console.error(`Failed to save to destination`, JSON.stringify(destination), `Error:`, error);
      }
    }, 8); // Process up to 8 destinations at once
    
  } catch (error) {
    console.error('Error processing webhook data:', error);
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