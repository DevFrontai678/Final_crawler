// scheduler.js
import cron from 'node-cron';
import { runGoogleJobsWorker } from './google-jobs-worker.js';
import winston from 'winston';

const logger = winston.createLogger({
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'scheduler.log' })
  ]
});

// Har roz subah 6 baje Priority 1 (Top 200) - Daily
cron.schedule('0 6 * * *', async () => {
  logger.info('⏰ Running Priority 1 (Daily)');
  await runGoogleJobsWorker(1, 50); // Sirf 50 per run, taake load na ho
});

// Har 3 din (Monday & Thursday) Priority 2
cron.schedule('0 8 * * 1,4', async () => {
  logger.info('⏰ Running Priority 2 (Every 3 days)');
  await runGoogleJobsWorker(2, 100);
});

// Har Sunday Priority 3 (Weekly)
cron.schedule('0 10 * * 0', async () => {
  logger.info('⏰ Running Priority 3 (Weekly)');
  await runGoogleJobsWorker(3, 150);
});

logger.info('✅ Scheduler started. Waiting for triggers...');

// Handle shutdown gracefully
process.on('SIGTERM', () => {
  logger.info('Shutting down scheduler...');
  process.exit(0);
});
