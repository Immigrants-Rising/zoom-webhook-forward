const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const axios = require('axios');
const Airtable = require('airtable');
const yaml = require('js-yaml');
const fs = require('fs');
const {google} = require('googleapis');

const app = express();

// Load configuration from YAML file
function loadConfig() {
  try {
    const configFile = process.env.CONFIG_FILE_PATH || './config.yaml';
    const fileContents = fs.readFileSync(configFile, 'utf8');
    return yaml.load(fileContents);
  } catch (e) {
    console.error('Error loading configuration:', e);
    return { bases: [] };
  }
}

// Function to determine which destinations to use based on meeting topic
function determineTargetBases(meetingTopic, config) {
  const matchingDestinations = [];
  
  // Process Airtable bases
  if (config.bases) {
    for (const base of config.bases) {
      // If the base has no condition, or if it meets the condition
      if (!base.condition || meetingTopic.match(new RegExp(base.condition, 'i'))) {
        // Set default priority if not specified
        base.priority = base.priority || 100;
        matchingDestinations.push(base);
      }
    }
  }
  
  // Process Google Sheets
  if (config.googlesheets) {
    for (const sheet of config.googlesheets) {
      // If the sheet has no condition, or if it meets the condition
      if (!sheet.condition || meetingTopic.match(new RegExp(sheet.condition, 'i'))) {
        // Set default priority if not specified
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
    const base = new Airtable({apiKey}).base(baseConfig.baseId);
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
  return withRetry(async () => {
    // Authenticate with Google Sheets API
    const auth = await getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });
    
    // Apply field mappings if specified
    const mappedRecord = applyFieldMappings(record, sheetConfig.fieldMappings);
    
    // First, get the column headers from the sheet to ensure proper ordering
    const headerResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetConfig.spreadsheetId,
      range: `${sheetConfig.sheetName}!1:1`,
    });
    
    const headers = headerResponse.data.values[0];
    
    // Create an ordered row based on the headers
    const rowData = headers.map(header => mappedRecord[header] || '');
    
    // Now find the last row with data to append after it
    const dataResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetConfig.spreadsheetId,
      range: sheetConfig.sheetName,
    });
    
    const lastRow = dataResponse.data.values ? dataResponse.data.values.length + 1 : 2;
    const range = `${sheetConfig.sheetName}!A${lastRow}`;
    
    // Append the new row
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetConfig.spreadsheetId,
      range: range,
      valueInputOption: 'RAW',
      resource: {
        values: [rowData],
      },
    });
    
    console.log(`Record saved to Google Sheet ${sheetConfig.spreadsheetId}, sheet ${sheetConfig.sheetName} at row ${lastRow}`);
    return { sheet: sheetConfig.sheetName, row: lastRow };
  }, `Google Sheet ${sheetConfig.spreadsheetId}`);
}

// Helper function to get Google Auth client
async function getGoogleAuth() {
  // There are multiple ways to authenticate with Google APIs
  // This example uses service account credentials from environment
  
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    // If using a credentials file path
    return new google.auth.GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
  } else if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
    // If credentials are provided as a JSON string in environment
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
    const client = new google.auth.JWT(
      credentials.client_email,
      null,
      credentials.private_key,
      ['https://www.googleapis.com/auth/spreadsheets']
    );
    await client.authorize();
    return client;
  } else if (process.env.FUNCTION_IDENTITY) {
    // If running on Google Cloud with proper IAM permissions (recommended approach)
    // This uses the default service account of the Cloud Function
    return new google.auth.GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });
  } else {
    throw new Error('No Google authentication method available. Please set up credentials.');
  }
}

app.use(bodyParser.json());

app.post('/', async (req, res) => {
  var response;

  console.log(req.headers);
  console.log(req.body);

  // construct the message string
  const message = `v0:${req.headers['x-zm-request-timestamp']}:${JSON.stringify(req.body)}`;

  const hashForVerify = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN).update(message).digest('hex');

  // hash the message string with your Webhook Secret Token and prepend the version semantic
  const signature = `v0=${hashForVerify}`;

  // you validating the request came from Zoom
  if (req.headers['x-zm-signature'] === signature) {

    // Zoom validating you control the webhook endpoint
    if(req.body.event === 'endpoint.url_validation') {
      const hashForValidate = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN).update(req.body.payload.plainToken).digest('hex');

      response = {
        message: {
          plainToken: req.body.payload.plainToken,
          encryptedToken: hashForValidate
        },
        status: 200
      };

      console.log(response.message);

      res.status(response.status);
      res.json(response.message);
    } else {
      response = { message: 'Authorized request to Zoom Webhook Catcher.', status: 200 };

      console.log(response.message);

      res.status(response.status);
      res.json(response);

      // Forward the webhook to another endpoint
      try {
        if (process.env.FORWARD_WEBHOOK_URL) {
          await axios.post(process.env.FORWARD_WEBHOOK_URL, req.body, {
            headers: {
              'Content-Type': 'application/json'
            }
          });
          console.log('Webhook forwarded successfully');
        }
        
        // Save data to Airtable if it's a relevant event
        if (req.body.event === 'webinar.participant_joined' || 
            req.body.event === 'webinar.participant_left' ||
            req.body.event === 'meeting.participant_joined' ||
            req.body.event === 'meeting.participant_left') {
          try {
            const record = mapWebhookDataToAirtableRecord(req.body);
            const meetingTopic = req.body.payload.object.topic || '';
            const config = loadConfig();
            const targetBases = determineTargetBases(meetingTopic, config);
            
            if (targetBases.length === 0) {
              console.log(`No matching bases found for topic: "${meetingTopic}"`);
              
              // Fallback to the environment variable settings if configured
              if (process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID && process.env.AIRTABLE_TABLE_ID) {
                const fallbackBase = {
                  baseId: process.env.AIRTABLE_BASE_ID,
                  tableId: process.env.AIRTABLE_TABLE_ID
                };
                console.log('Using fallback Airtable configuration from environment variables');
                await saveToAirtable(record, fallbackBase, process.env.AIRTABLE_API_KEY);
              }
            } else {
              // Save to all matching destinations with rate limiting protection
              const apiKey = process.env.AIRTABLE_API_KEY;
              
              // Process destinations sequentially to avoid overwhelming rate limits
              for (const destination of targetBases) {
                try {
                  if (destination.type === 'googlesheet') {
                    // Handle Google Sheet destination
                    await saveToGoogleSheet(record, destination);
                    console.log(`Webhook data saved to Google Sheet ${destination.spreadsheetId} for event: ${req.body.event}`);
                  } else {
                    // Default to Airtable destination
                    await saveToAirtable(record, destination, apiKey);
                    console.log(`Webhook data saved to Airtable base ${destination.baseId} for event: ${req.body.event}`);
                  }
                } catch (error) {
                  console.error(`Failed to save to destination ${JSON.stringify(destination)} after multiple attempts:`, error);
                }
              }
            }
          } catch (error) {
            console.error('Error saving webhook data to Airtable:', error);
          }
        }
      } catch (error) {
        console.error('Error processing webhook:', error);
      }
    }
  } else {
    response = { message: 'Unauthorized request to Zoom Webhook Catcher.', status: 401 };

    console.log(response.message);

    res.status(response.status);
    res.json(response);
  }
});

functions.http('zoomWebhook', app);