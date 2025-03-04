const fs = require('fs');
const Airtable = require('airtable');
require('dotenv').config(); // For loading environment variables

// Configure Airtable - same as in your cloud function
const base = new Airtable({apiKey: process.env.AIRTABLE_API_KEY}).base(process.env.AIRTABLE_BASE_ID);
const tableName = process.env.AIRTABLE_TABLE_ID;
const table = base(tableName);

// Function to ensure proper handling of Unicode characters
function decodeUnicodeString(str) {
  // If the string is undefined or null, return an empty string
  if (!str) return '';
  
  // If it's already a proper string (not a JSON string with escape sequences), return as is
  // This handles cases where the JSON parser has already converted the escape sequences
  return str;
}

// Reusing your existing mapping function with Unicode handling
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
    "Meeting Topic": decodeUnicodeString(object.topic),
    "Meeting Type": object.type,
    "Start Time": object.start_time,
    "Timezone": object.timezone,
    "Duration": object.duration,
    "Participant User ID": participant.user_id,
    "Participant Name": decodeUnicodeString(participant.user_name),
    "Participant ID": participant.id,
    "Participant UUID": participant.participant_uuid,
    "Email": decodeUnicodeString(participant.email),
    "Registrant ID": participant.registrant_id,
    // This seems to be the same as participant.id
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

// Modified function to save record to Airtable with Promise handling
function saveToAirtable(record) {
  return new Promise((resolve, reject) => {
    table.create([{ fields: record }], function(err, records) {
      if (err) {
        console.error('Error saving to Airtable:', err);
        return reject(err);
      }
      console.log('Record saved to Airtable:', records[0].getId());
      resolve(records[0]);
    });
  });
}

// Function to process the JSON file with throttling
async function processWebhookData(filePath, batchSize = 5, delayMs = 1000) {
  try {
    // Read and parse the JSON file
    const rawData = fs.readFileSync(filePath, 'utf8');
    
    // Log a sample of the raw data to verify encoding
    console.log('First 200 characters of raw data:');
    console.log(rawData.slice(0, 200));
    
    // Log a sample of special characters to check encoding
    if (rawData.includes('\\u00')) {
      console.log('Found Unicode escape sequences in the raw data');
      const sampleMatch = rawData.match(/[^\\]\\u00[0-9a-f]{2}/);
      if (sampleMatch) {
        console.log(`Sample Unicode escape: ${sampleMatch[0]}`);
      }
    }
    
    const webhookDataArray = JSON.parse(rawData);
    
    console.log(`Found ${webhookDataArray.length} webhook events to process`);
    
    // Filter to only include the relevant event types
    const relevantEvents = webhookDataArray.filter(webhook => 
      webhook.event === 'webinar.participant_joined' || 
      webhook.event === 'webinar.participant_left' ||
      webhook.event === 'meeting.participant_joined' ||
      webhook.event === 'meeting.participant_left'
    );
    
    console.log(`Processing ${relevantEvents.length} relevant events`);

    // Add a verification step for a few records before processing
    console.log('\nVerifying Unicode characters in the first webhook:');
    if (relevantEvents.length > 0) {
      const sample = relevantEvents[0];
      if (sample.payload && sample.payload.object && sample.payload.object.participant) {
        const participantName = sample.payload.object.participant.user_name;
        console.log(`Original participant name: ${participantName}`);
        
        // Check if there are any remaining Unicode escape sequences
        if (participantName && participantName.includes('\\u')) {
          console.log('Warning: Unicode escape sequences still present in the parsed data');
          console.log('This might indicate that the JSON was double-escaped');
        } else {
          console.log('Unicode characters appear to be correctly parsed');
        }
      }
    }
    
    // Process in batches to avoid overwhelming the API
    let processed = 0;
    let successful = 0;
    let failed = 0;
    
    for (let i = 0; i < relevantEvents.length; i += batchSize) {
      const batch = relevantEvents.slice(i, i + batchSize);
      console.log(`Processing batch ${Math.floor(i/batchSize) + 1} of ${Math.ceil(relevantEvents.length/batchSize)}`);
      
      const promises = batch.map(async (webhookData) => {
        try {
          const record = mapWebhookDataToAirtableRecord(webhookData);
          await saveToAirtable(record);
          successful++;
          return { success: true, event: webhookData.event };
        } catch (error) {
          failed++;
          return { success: false, event: webhookData.event, error: error.message };
        }
      });
      
      const results = await Promise.all(promises);
      processed += batch.length;
      
      // Log the results of this batch
      results.forEach((result, index) => {
        if (result.success) {
          console.log(`✅ Successfully processed ${result.event} (${i + index + 1}/${relevantEvents.length})`);
        } else {
          console.error(`❌ Failed to process ${result.event} (${i + index + 1}/${relevantEvents.length}): ${result.error}`);
        }
      });
      
      console.log(`Progress: ${processed}/${relevantEvents.length} (${Math.round(processed/relevantEvents.length*100)}%)`);
      
      // Wait before processing the next batch to avoid rate limits
      if (i + batchSize < relevantEvents.length) {
        console.log(`Waiting ${delayMs}ms before next batch...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
    
    console.log('\nBackfill Summary:');
    console.log(`Total processed: ${processed}`);
    console.log(`Successfully added: ${successful}`);
    console.log(`Failed: ${failed}`);
    
  } catch (error) {
    console.error('Error processing webhook data:', error);
  }
}

// Function to test Unicode decoding
function testUnicodeHandling() {
  const testString = 'Connor L\\u00f3pez A\\u00f1ejo';
  console.log('Original escaped string:', testString);
  
  // When the JSON parser processes the file, it will automatically convert
  // the Unicode escape sequences to actual characters
  const parsed = JSON.parse(`"${testString}"`);
  console.log('After JSON parsing:', parsed);
  
  // Our decoder function should handle this correctly
  console.log('After our decodeUnicodeString function:', decodeUnicodeString(parsed));
  
  return parsed;
}

// Main execution
// Specify the path to your JSON file
const jsonFilePath = process.argv[2];

if (!jsonFilePath) {
  console.error('Please provide the path to the JSON file as a command line argument');
  console.log('Usage: node backfill-script.js path/to/webhooks.json');
  process.exit(1);
}

// Run a test to verify Unicode handling
console.log('Testing Unicode handling:');
const testResult = testUnicodeHandling();
console.log(`Result should show "López Muñoz": "${testResult}"`);
console.log('-'.repeat(50));

console.log(`Starting backfill process for file: ${jsonFilePath}`);
processWebhookData(jsonFilePath)
  .then(() => console.log('Backfill process completed'))
  .catch(err => console.error('Backfill process failed:', err));