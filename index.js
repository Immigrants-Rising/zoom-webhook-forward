const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');
const axios = require('axios');

const app = express();

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
      response = { message: 'Authorized request to Zoom Webhook sample.', status: 200 };

      console.log(response.message);

      res.status(response.status);
      res.json(response);

      // Forward the webhook to another endpoint
      try {
        await axios.post(process.env.FORWARD_WEBHOOK_URL, req.body, {
          headers: {
            'Content-Type': 'application/json'
          }
        });
        console.log('Webhook forwarded successfully');
      } catch (error) {
        console.error('Error forwarding webhook:', error);
      }
    }
  } else {
    response = { message: 'Unauthorized request to Zoom Webhook sample.', status: 401 };

    console.log(response.message);

    res.status(response.status);
    res.json(response);
  }
});

functions.http('zoomWebhook', app);