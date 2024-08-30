const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const crypto = require('crypto');

const app = express();
app.use(bodyParser.json());

app.get('/', (req, res) => {
  res.status(200).send(`Zoom Webhook sample successfully running. Set this URL with the /webhook path as your apps Event notification endpoint URL.`);
});

app.post('/webhook', (req, res) => {
  let response;

  console.log(req.headers);
  console.log(req.body);

  const message = `v0:${req.headers['x-zm-request-timestamp']}:${JSON.stringify(req.body)}`;
  const hashForVerify = crypto.createHmac('sha256', process.env.ZOOM_WEBHOOK_SECRET_TOKEN).update(message).digest('hex');
  const signature = `v0=${hashForVerify}`;

  if (req.headers['x-zm-signature'] === signature) {
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
      res.status(response.status).json(response.message);
    } else {
      response = { message: 'Authorized request to Zoom Webhook sample.', status: 200 };
      console.log(response.message);
      res.status(response.status).json(response);
    }
  } else {
    response = { message: 'Unauthorized request to Zoom Webhook sample.', status: 401 };
    console.log(response.message);
    res.status(response.status).json(response);
  }
});

functions.http('zoomWebhook', app);