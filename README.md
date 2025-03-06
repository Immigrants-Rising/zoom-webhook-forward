# Zoom Webhook Handler

A highly optimized Google Cloud Function for processing Zoom meeting and webinar webhooks and routing the data to multiple Airtable bases and Google Sheets. You can also forward to an arbitrary endpoint.

This is originally forked from [this repository](https://github.com/zoom/webhook-sample).

## Overview

This project provides a serverless solution for capturing Zoom webhook events (like participant joins/leaves) and automatically saving the data to configured destinations based on meeting topics. It features intelligent routing, performance optimizations, and comprehensive error handling.

## Features

- **Multi-destination Support**: Route data to multiple Airtable bases and Google Sheets
- **Conditional Routing**: Use regex patterns to route data based on meeting topics
- **Performance Optimized**: Header prefetching, caching, and controlled concurrency
- **Rate Limit Handling**: Automatic retries with exponential backoff
- **YAML Configuration**: Easy-to-maintain configuration file
- **Field Mapping**: Custom field mappings for each destination
- **Detailed Logging**: Granular timing and performance metrics
- **Data Backfill**: Utility for recovering from outages
- **Status Monitoring**: Endpoint for checking system health

## Components

### 1. Webhook Handler (Cloud Function)

The main component that receives webhooks from Zoom and routes them to appropriate destinations.

### 2. Configuration (YAML)

Defines routing rules, destinations, and field mappings.

### 3. Backfill Utility

A standalone script for reprocessing webhook data from a JSON file.

## Setup & Deployment

### Prerequisites

- Google Cloud account with Cloud Functions enabled
- Airtable account and API key
- Google service account with Sheets API access
- Zoom developer account with webhook configuration

### Environment Variables

```
# Required
AIRTABLE_API_KEY=your_airtable_api_key
ZOOM_WEBHOOK_SECRET_TOKEN=your_zoom_webhook_secret

# Optional
FORWARD_WEBHOOK_URL=https://another-endpoint.com/webhook
CONFIG_FILE_PATH=./config.yaml
FUNCTION_IDENTITY=true  # For using default identity

# For Google Sheets (choose one)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
GOOGLE_SERVICE_ACCOUNT_KEY={"type":"service_account",...}
```

### Configuration File (config.yaml)

```yaml
bases:
  # Airtable destinations
  - baseId: appXXXXXXXXXXXXXX
    tableId: tblXXXXXXXXXXXXXX
    condition: "\\[HE\\]"
    priority: 10
    fieldMappings:
      "Meeting Topic": "Session Name"

googlesheets:
  # Google Sheets destinations
  - spreadsheetId: "1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    sheetName: "Zoom Attendance"
    condition: "\\[HE\\]"
    priority: 20
    fieldMappings:
      "Participant Name": "user_name"
```

### Deployment

```bash
# Install dependencies
npm install

# Deploy to Google Cloud Functions
gcloud functions deploy zoomWebhook \
  --runtime nodejs22 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point zoomWebhook \
  --env-vars-file .env.yaml
```

## Usage

### Zoom Webhook Setup

1. Create a Zoom App in the Zoom Marketplace
2. Configure Feature → Event Subscriptions
3. Add the deployed function URL as the endpoint
4. Subscribe to these events:
   - `meeting.participant_joined`
   - `meeting.participant_left`
   - `webinar.participant_joined`
   - `webinar.participant_left`

### Google Sheets Permissions

For each Google Sheet in your configuration:
1. Open the sheet in Google Sheets
2. Click "Share"
3. Add the service account email with Editor permissions

## Data Backfill

If your function experiences downtime, you can use the backfill utility to reprocess missed events.

## Performance Considerations

- **Cold Starts**: The function performs initialization at cold start to minimize per-request overhead
- **Concurrency**: Default concurrency limit is 3 destinations at once
- **Rate Limits**: Airtable has a 5 QPS limit per base
- **Headers Prefetching**: Google Sheets headers are prefetched during initialization
- **Response Time**: The function responds to Zoom immediately and processes asynchronously

## Troubleshooting

### Common Issues

1. **Unauthorized requests from Zoom**:
   - Verify the `ZOOM_WEBHOOK_SECRET_TOKEN` matches your Zoom app configuration

2. **Missing Google Sheets data**:
   - Ensure the service account has edit access to the spreadsheet
   - Check header mapping in configuration matches actual sheet headers

3. **Rate limiting errors**:
   - Reduce concurrency or add more delays between operations
   - Check for other processes accessing the same Airtable bases

4. **Slow performance**:
   - Inspect timing logs to identify bottlenecks
   - Consider splitting high-traffic destinations

### Checking Logs

```bash
gcloud functions logs read zoomWebhook --limit 100
```

Look for `[TIMING]` entries to identify performance bottlenecks.

## Advanced Configuration

### Custom Field Mappings

Each destination can have custom field mappings to transform field names:

```yaml
fieldMappings:
  "Meeting Topic": "Session Name"
  "Participant Name": "Attendee Name"
  "Email": "Contact Email"
```

### Destination Priority

Lower priority numbers are processed first:

```yaml
- baseId: appXXXXXXXXXXXXXX
  priority: 10  # Processed first

- spreadsheetId: "1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
  priority: 20  # Processed second
```
