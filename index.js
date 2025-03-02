const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const axios = require('axios');
const Airtable = require('airtable');
const yaml = require('js-yaml');
const fs = require('fs');

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

// Function to determine which base to use based on meeting topic
function determineTargetBases(meetingTopic, config) {
  const matchingBases = [];
  
  for (const base of config.bases) {
    // If the base has no condition, or if it meets the condition
    if (!base.condition || meetingTopic.match(new RegExp(base.condition, 'i'))) {
      // Set default priority if not specified
      if (base.priority === undefined) {
        base.priority = 100;
      }
      matchingBases.push(base);
    }
  }
  
  // Sort bases by priority (lower numbers first)
  return matchingBases.sort((a, b) => (a.priority || 100) - (b.priority || 100));
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

// Function to save record to a specific Airtable base and table with retry logic
async function saveToAirtable(record, baseConfig, apiKey, attempt = 1) {
  const MAX_ATTEMPTS = 5;
  const BASE_DELAY_MS = 1000;
  
  try {
    const base = new Airtable({apiKey}).base(baseConfig.baseId);
    const table = base(baseConfig.tableId);
    
    // Apply field mappings if specified
    let mappedRecord = {...record};
    if (baseConfig.fieldMappings) {
      mappedRecord = Object.keys(record).reduce((mapped, key) => {
        const mappedKey = baseConfig.fieldMappings[key] || key;
        mapped[mappedKey] = record[key];
        return mapped;
      }, {});
    }
    
    // Save the record to Airtable
    return new Promise((resolve, reject) => {
      table.create(
        [{ fields: mappedRecord }],
        function (err, records) {
          // Handle rate limiting (429) errors with exponential backoff
          if (err && err.statusCode === 429 && attempt < MAX_ATTEMPTS) {
            const delay = Math.min(BASE_DELAY_MS * Math.pow(2, attempt - 1), 30000); // Cap at 30 seconds
            console.log(`Rate limit hit for base ${baseConfig.baseId}. Retrying in ${delay}ms (attempt ${attempt}/${MAX_ATTEMPTS})`);
            
            setTimeout(() => {
              saveToAirtable(record, baseConfig, apiKey, attempt + 1)
                .then(resolve)
                .catch(reject);
            }, delay);
            return;
          } else if (err) {
            console.error(`Error saving to Airtable (attempt ${attempt}/${MAX_ATTEMPTS}):`, err);
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
  } catch (error) {
    console.error('Error saving to Airtable:', error);
    throw error;
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
              // Save to all matching bases with rate limiting protection
              const apiKey = process.env.AIRTABLE_API_KEY;
              
              // Process bases sequentially to avoid overwhelming Airtable rate limits
              // This is especially important if multiple conditions match the same base
              for (const base of targetBases) {
                try {
                  await saveToAirtable(record, base, apiKey);
                  console.log(`Webhook data saved to Airtable base ${base.baseId} for event: ${req.body.event}`);
                } catch (error) {
                  console.error(`Failed to save to Airtable base ${base.baseId} after multiple attempts:`, error);
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