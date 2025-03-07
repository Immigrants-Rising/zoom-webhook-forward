const winston = require('winston');
const crypto = require('crypto');

// Imports the Google Cloud client library for Winston
const {LoggingWinston} = require('@google-cloud/logging-winston');

// Creates a client
const loggingWinston = new LoggingWinston();

// Define log levels and colors
const logLevels = {
  levels: {
    error: 0,   // Critical errors that require immediate attention
    warn: 1,    // Warning conditions that should be reviewed
    info: 2,    // General informational messages about system operation
    timing: 3,  // Timing information for performance tracking
    debug: 4,   // Detailed information for debugging
    silly: 5    // Extra verbose information for development
  },
  colors: {
    error: 'red',
    warn: 'yellow',
    info: 'green',
    timing: 'cyan',
    debug: 'blue',
    silly: 'magenta'
  }
};

// Apply colors to Winston
winston.addColors(logLevels.colors);

// Store request IDs for traceability
const requestStore = new Map();
const REQUEST_ID_TIMEOUT = 30 * 60 * 1000; // Clear request IDs after 30 minutes

// Periodically clean up old request IDs to prevent memory leaks
setInterval(() => {
  const now = Date.now();
  for (const [requestId, timestamp] of requestStore.entries()) {
    if (now - timestamp > REQUEST_ID_TIMEOUT) {
      requestStore.delete(requestId);
    }
  }
}, 5 * 60 * 1000); // Run cleanup every 5 minutes

// Function to generate or retrieve a request ID
function getRequestId(requestIdFromHeader) {
  let requestId;
  
  if (requestIdFromHeader) {
    // Use existing request ID from header if provided
    requestId = requestIdFromHeader;
  } else {
    // Generate a new unique request ID
    requestId = crypto.randomUUID();
  }
  
  // Store the request ID with timestamp for cleanup
  requestStore.set(requestId, Date.now());
  
  return requestId;
}

// Create the logger instance
const logger = winston.createLogger({
  levels: logLevels.levels,
  level: process.env.LOG_LEVEL || 'info', // Default to info, can be overridden by environment variable
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.printf(({ timestamp, level, message, requestId, meetingId, eventType, ...rest }) => {
      // Format metadata for better readability
      const metaFields = [];
      
      // Add request ID if available
      if (requestId) {
        metaFields.push(`reqId=${requestId}`);
      }
      
      // Add meeting ID if available
      if (meetingId) {
        metaFields.push(`mtgId=${meetingId}`);
      }
      
      // Add event type if available
      if (eventType) {
        metaFields.push(`event=${eventType}`);
      }
      
      // Add metadata prefix
      const metaPrefix = metaFields.length > 0 ? `[${metaFields.join('|')}] ` : '';
      
      // Format any remaining metadata
      const restString = Object.keys(rest).length ? ` ${JSON.stringify(rest)}` : '';
      
      return `${timestamp} [${level.toUpperCase()}] ${metaPrefix}${message}${restString}`;
    })
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize({ all: true }),
        winston.format.printf(({ timestamp, level, message, requestId, meetingId, eventType, ...rest }) => {
          // Format metadata for better readability
          const metaFields = [];
          
          // Add request ID if available
          if (requestId) {
            metaFields.push(`reqId=${requestId}`);
          }
          
          // Add meeting ID if available
          if (meetingId) {
            metaFields.push(`mtgId=${meetingId}`);
          }
          
          // Add event type if available
          if (eventType) {
            metaFields.push(`event=${eventType}`);
          }
          
          // Add metadata prefix
          const metaPrefix = metaFields.length > 0 ? `[${metaFields.join('|')}] ` : '';
          
          // Format any remaining metadata
          const restString = Object.keys(rest).length ? ` ${JSON.stringify(rest)}` : '';
          
          return `${timestamp} [${level.toUpperCase()}] ${metaPrefix}${message}${restString}`;
        })
      )
    }),
    loggingWinston
  ]
});

// Add timing convenience method for existing [TIMING] logs
logger.timing = logger.timing || function(message, meta) {
  return logger.log('timing', message, meta);
};

/**
 * Set logging level dynamically
 * @param {string} level - One of 'error', 'warn', 'info', 'timing', 'debug', 'silly'
 */
function setLogLevel(level) {
  if (logLevels.levels[level] !== undefined) {
    logger.level = level;
    logger.info(`Logging level set to: ${level}`);
  } else {
    logger.warn(`Invalid log level: ${level}. Using default.`);
  }
}

module.exports = {
  logger,
  setLogLevel,
  getRequestId
};