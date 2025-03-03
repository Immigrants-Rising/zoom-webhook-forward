const winston = require('winston');

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

// Create the logger instance
const logger = winston.createLogger({
  levels: logLevels.levels,
  level: process.env.LOG_LEVEL || 'info', // Default to info, can be overridden by environment variable
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.printf(({ timestamp, level, message, ...rest }) => {
      const restString = Object.keys(rest).length ? ` ${JSON.stringify(rest)}` : '';
      return `${timestamp} [${level.toUpperCase()}] ${message}${restString}`;
    })
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize({ all: true }),
        winston.format.printf(({ timestamp, level, message, ...rest }) => {
          const restString = Object.keys(rest).length ? ` ${JSON.stringify(rest)}` : '';
          return `${timestamp} [${level.toUpperCase()}] ${message}${restString}`;
        })
      )
    })
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
  setLogLevel
};