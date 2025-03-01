const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const axios = require('axios');
const Airtable = require('airtable');

const app = express();

// Configure Airtable
const base = new Airtable({apiKey: process.env.AIRTABLE_API_KEY}).base(process.env.AIRTABLE_BASE_ID);

// Function to map webhook data to Airtable record
function mapWebhookDataToAirtableRecord(webhookData) {
  const eventType = webhookData.event;
  const payload = webhookData.payload;
  const object = payload.object;
  const participant = object.participant;
  
  // Base record with common fields
  const record = {
    "ID": participant.id,
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
    // Map join_time to Event Datetime
    record["Event Datetime"] = participant.join_time;
  } else if (eventType === "webinar.participant_left") {
    record["Leave Time"] = participant.leave_time;
    record["Leave Reason"] = participant.leave_reason;
    record["Event Datetime"] = participant.leave_time;
  } else if (eventType === "meeting.participant_joined") {
    // For meeting joins, use date_time field
    record["Event Datetime"] = participant.date_time;
  } else if (eventType === "meeting.participant_left") {
    record["Leave Time"] = participant.leave_time;
    record["Leave Reason"] = participant.leave_reason;
    record["Event Datetime"] = participant.date_time;
  }
  
  return record;
}

// Function to save record to Airtable
async function saveToAirtable(record) {
  try {
    // Get table name from environment variables
    const tableName = process.env.AIRTABLE_TABLE_ID;
    const table = base(tableName);
    
    // Create a record in Airtable
    const result = await table.create([{ fields: record }]);
    console.log('Record saved to Airtable:', result.getId());
    return result;
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

  // you validating the request came from Zoom https://marketplace.zoom.us/docs/api-reference/webhook-reference#notification-structure
  if (req.headers['x-zm-signature'] === signature) {

    // Zoom validating you control the webhook endpoint https://marketplace.zoom.us/docs/api-reference/webhook-reference#validate-webhook-endpoint
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
            await saveToAirtable(record);
            console.log(`Webhook data saved to Airtable for event: ${req.body.event}`);
          } catch (error) {
            console.error('Error saving webhook data to Airtable:', error);
          }
        }
      } catch (error) {
        console.error('Error forwarding webhook:', error);
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