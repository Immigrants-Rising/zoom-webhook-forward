#!/usr/bin/env node

/**
 * Webhook Data Backfill Script
 * 
 * This script processes webhook data from a JSON file and backfills
 * Airtable bases and Google Sheets with the data.
 * 
 * Usage:
 *   node backfill.js path/to/webhook-data.json
 * 
 * The JSON file should contain an array of webhook objects in the same format
 * as received from Zoom.
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const Airtable = require('airtable');
const {google} = require('googleapis');
const dotenv = require('dotenv');

// Load environment variables from .env file
dotenv.config();

// Global variables
let configCache = null;
let googleAuthClient = null;
let sheetsApiClient = null;
const airtableBaseCache = new Map();
const sheetsHeadersCache = new Map();

// Process command line arguments
const args = process.argv.slice(2);
if (args.length === 0) {
  console.error('Error: Missing JSON file path');
  console.error('Usage: node backfill.js path/to/webhook-data.json');
  process.exit(1);
}

const jsonFilePath = args[0];
if (!fs.existsSync(jsonFilePath)) {
  console.error(`Error: File not found: ${jsonFilePath}`);
  process.exit(1);
}

// Main function
async function main() {
  try {
    console.log('Starting webhook data backfill...');
    
    // Initialize
    await initialize();
    
    // Load webhook data from JSON file
    console.log(`Loading webhook data from ${jsonFilePath}`);
    const webhookData = JSON.parse(fs.readFileSync(jsonFilePath, 'utf8'));
    
    if (!Array.isArray(webhookData)) {
      throw new Error('JSON file must contain an array of webhook objects');
    }
    
    console.log(`Loaded ${webhookData.length} webhook events for processing`);
    
    // Process each webhook event
    let successCount = 0;
    let failureCount = 0;
    
    for (let i = 0; i < webhookData.length; i++) {
      const webhook = webhookData[i];
      const eventNumber = i + 1;
      
      console.log(`\nProcessing event ${eventNumber}/${webhookData.length}: ${webhook.event}`);
      
      try {
        await processWebhook(webhook);
        successCount++;
        console.log(`✓ Event ${eventNumber} processed successfully`);
      } catch (error) {
        failureCount++;
        console.error(`✗ Failed to process event ${eventNumber}:`, error);
      }
      
      // Add a small delay to avoid overwhelming APIs
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log('\nBackfill complete!');
    console.log(`Processed ${webhookData.length} events: ${successCount} succeeded, ${failureCount} failed`);
    
  } catch (error) {
    console.error('Error during backfill:', error);
    process.exit(1);
  }
}

// Load configuration
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
    
    console.log(`Loaded ${config.bases.length} Airtable bases and ${config.googlesheets.length} Google Sheets configurations`);
    configCache = config;
    return configCache;
  } catch (e) {
    console.error('Error loading configuration:', e);
    configCache = { bases: [], googlesheets: [] };
    return configCache;
  }
}

// Initialize Google auth
async function initGoogleAuth() {
  if (googleAuthClient) {
    return googleAuthClient;
  }
  
  try {
    console.log('Initializing Google auth...');
    
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

// Initialization function
async function initialize() {
  console.log('Initializing...');
  const startTime = Date.now();
  
  // Load config
  const config = loadConfig();
  
  // Initialize Google auth if needed
  if (config.googlesheets && config.googlesheets.length > 0) {
    await initGoogleAuth();
    
    // Prefetch all sheet headers
    console.log(`Prefetching headers for ${config.googlesheets.length} Google Sheets...`);
    
    for (const sheet of config.googlesheets) {
      try {
        console.log(`Prefetching headers for sheet: ${sheet.sheetName}`);
        const headerResponse = await sheetsApiClient.spreadsheets.values.get({
          spreadsheetId: sheet.spreadsheetId,
          range: `${sheet.sheetName}!1:1`,
        });
        
        if (headerResponse.data.values && headerResponse.data.values[0]) {
          sheetsHeadersCache.set(
            `${sheet.spreadsheetId}_${sheet.sheetName}`, 
            headerResponse.data.values[0]
          );
        }
      } catch (error) {
        console.error(`Error prefetching headers for ${sheet.sheetName}:`, error);
      }
    }
  }
  
  console.log(`Initialization completed in ${Date.now() - startTime}ms`);
}

// Get cached Airtable base
function getAirtableBase(apiKey, baseId) {
  const cacheKey = `${apiKey}_${baseId}`;
  
  if (!airtableBaseCache.has(cacheKey)) {
    const base = new Airtable({apiKey}).base(baseId);
    airtableBaseCache.set(cacheKey, base);
    return base;
  }
  
  return airtableBaseCache.get(cacheKey);
}

// Apply field mappings to a record
function applyFieldMappings(record, fieldMappings) {
  if (!fieldMappings) return {...record};
  
  return Object.keys(record).reduce((mapped, key) => {
    const mappedKey = fieldMappings[key] || key;
    mapped[mappedKey] = record[key];
    return mapped;
  }, {});
}

// Function to save record to Airtable with retries
async function saveToAirtable(record, baseConfig, apiKey) {
  const MAX_ATTEMPTS = 5;
  const RETRY_DELAY_MS = 1000;
  
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const base = getAirtableBase(apiKey, baseConfig.baseId);
      const table = base(baseConfig.tableId);
      
      // Apply field mappings
      const mappedRecord = applyFieldMappings(record, baseConfig.fieldMappings);
      
      // Save to Airtable
      return await new Promise((resolve, reject) => {
        table.create(
          [{ fields: mappedRecord }],
          function (err, records) {
            if (err) {
              reject(err);
              return;
            }
            resolve(records);
          }
        );
      });
    } catch (error) {
      // Check if it's a rate limit error
      const isRateLimitError = error.statusCode === 429;
      
      if (isRateLimitError && attempt < MAX_ATTEMPTS) {
        const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
        console.log(`Rate limit hit for Airtable base ${baseConfig.baseId}. Retrying in ${delay}ms (attempt ${attempt}/${MAX_ATTEMPTS})`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else if (attempt === MAX_ATTEMPTS) {
        throw error;
      } else {
        throw error;
      }
    }
  }
}

// Function to save record to Google Sheet with retries
async function saveToGoogleSheet(record, sheetConfig) {
  const MAX_ATTEMPTS = 5;
  const RETRY_DELAY_MS = 1000;
  
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      // Apply field mappings
      const mappedRecord = applyFieldMappings(record, sheetConfig.fieldMappings);
      
      // Get headers from cache or fetch them
      const headersCacheKey = `${sheetConfig.spreadsheetId}_${sheetConfig.sheetName}`;
      let headers = sheetsHeadersCache.get(headersCacheKey);
      
      if (!headers) {
        console.log(`Headers not prefetched for ${sheetConfig.sheetName}, fetching now`);
        const headerResponse = await sheetsApiClient.spreadsheets.values.get({
          spreadsheetId: sheetConfig.spreadsheetId,
          range: `${sheetConfig.sheetName}!1:1`,
        });
        
        headers = headerResponse.data.values[0];
        sheetsHeadersCache.set(headersCacheKey, headers);
      }
      
      // Create ordered row
      const rowData = headers.map(header => mappedRecord[header] || '');
      
      // Append to sheet
      const appendResponse = await sheetsApiClient.spreadsheets.values.append({
        spreadsheetId: sheetConfig.spreadsheetId,
        range: `${sheetConfig.sheetName}!A1`,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        resource: {
          values: [rowData],
        },
      });
      
      return appendResponse;
    } catch (error) {
      // Check if it's a rate limit error
      const isRateLimitError = 
        error.code === 429 || 
        (error.response && error.response.status === 429) ||
        (error.message && error.message.includes('quota'));
      
      if (isRateLimitError && attempt < MAX_ATTEMPTS) {
        const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
        console.log(`Rate limit hit for Google Sheet ${sheetConfig.spreadsheetId}. Retrying in ${delay}ms (attempt ${attempt}/${MAX_ATTEMPTS})`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else if (attempt === MAX_ATTEMPTS) {
        throw error;
      } else {
        throw error;
      }
    }
  }
}

// Map webhook data to record
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

// Function to determine which destinations to use
function determineTargetBases(meetingTopic, config) {
  const matchingDestinations = [];
  
  // Process Airtable bases
  if (config.bases) {
    for (const base of config.bases) {
      let matches = false;
      
      // If the base has no condition, consider it a match
      if (!base.condition) {
        matches = true;
      } else {
        // Try to match the condition
        try {
          const regex = new RegExp(base.condition, 'i');
          matches = regex.test(meetingTopic);
        } catch (error) {
          console.error(`Invalid regex in base ${base.baseId} condition: ${base.condition}`, error);
          continue;
        }
      }
      
      if (matches) {
        // Set default priority
        base.priority = base.priority || 100;
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
      } else {
        // Try to match the condition
        try {
          const regex = new RegExp(sheet.condition, 'i');
          matches = regex.test(meetingTopic);
        } catch (error) {
          console.error(`Invalid regex in sheet ${sheet.spreadsheetId} condition: ${sheet.condition}`, error);
          continue;
        }
      }
      
      if (matches) {
        // Set default priority
        sheet.priority = sheet.priority || 100;
        // Add type identifier
        sheet.type = 'googlesheet';
        matchingDestinations.push(sheet);
      }
    }
  }
  
  // Sort destinations by priority (lower numbers first)
  return matchingDestinations.sort((a, b) => (a.priority || 100) - (b.priority || 100));
}

// Process a single webhook event
async function processWebhook(webhook) {
  const relevantEvents = [
    'webinar.participant_joined',
    'webinar.participant_left',
    'meeting.participant_joined',
    'meeting.participant_left'
  ];
  
  if (!relevantEvents.includes(webhook.event)) {
    console.log(`Skipping event type: ${webhook.event} (not a relevant event)`);
    return;
  }
  
  // Map the webhook data to a record
  const record = mapWebhookDataToAirtableRecord(webhook);
  const meetingTopic = webhook.payload.object.topic || '';
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
  
  // Process destinations sequentially to avoid overwhelming APIs
  console.log(`Processing ${targetBases.length} destinations`);
  const apiKey = process.env.AIRTABLE_API_KEY;
  
  for (const destination of targetBases) {
    const destType = destination.type === 'googlesheet' ? 'GoogleSheet' : 'Airtable';
    const destId = destination.type === 'googlesheet' 
      ? `${destination.spreadsheetId}/${destination.sheetName}`
      : `${destination.baseId}/${destination.tableId}`;
    
    console.log(`Processing ${destType}: ${destId}`);
    
    try {
      if (destination.type === 'googlesheet') {
        await saveToGoogleSheet(record, destination);
        console.log(`✓ Data saved to Google Sheet ${destId}`);
      } else {
        await saveToAirtable(record, destination, apiKey);
        console.log(`✓ Data saved to Airtable ${destId}`);
      }
    } catch (error) {
      console.error(`✗ Failed to save to ${destType} ${destId}:`, error);
      throw error; // Rethrow to be caught by the main loop
    }
  }
}

// Run the script
main().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
