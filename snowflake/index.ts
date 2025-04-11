import { printInputCommands, populateFruitsTableBulk, populateFruitsTable, deleteAllRowsFromTable, singletonTableMerge, getAllTableRows, bulkMerge, pruneFruits } from "./utils";
import { logger } from "./logger";
import SnowflakeConnection from "./connection";
import * as dotenv from 'dotenv';

dotenv.config();

const run = async () => {

logger.info('Starting snowflake run');

logger.info('Establishing Snowflake Connection...');
const snowflakeConnection = new SnowflakeConnection({
  account: process.env.SNOWFLAKE_ACCOUNT!,
  username: process.env.SNOWFLAKE_USER,
  authenticator: process.env.SNOWFLAKE_AUTHENTICATOR,
  role: process.env.SNOWFLAKE_ROLE,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
});
await snowflakeConnection.connect();
logger.success('Snowflake connection successfully established');

let input: string = printInputCommands().toLowerCase();

const tableName = process.env.SNOWFLAKE_TABLE ?? 'TEST_TABLE';

try {
while (input !== 'x') {
  switch (input) {
    case 'bp':
      await populateFruitsTableBulk(snowflakeConnection, tableName);
      break;
    case 'bm':
      await bulkMerge(snowflakeConnection, tableName);
      break;
    case 'p':
      await populateFruitsTable(snowflakeConnection, tableName);
      break;
    case 'g':
      const rows = await getAllTableRows(snowflakeConnection, tableName);
      console.table(rows);
      break;
    case 's':
      await singletonTableMerge(snowflakeConnection, tableName);
      break;
    case 'd':
      await deleteAllRowsFromTable(snowflakeConnection, tableName);
      break;
    case 'f':
      logger.info(pruneFruits());
      break;
    default:
      logger.error('Invalid input');
  }
  input = printInputCommands().toLowerCase();
};
} catch (error) {
  await snowflakeConnection.disconnect();
  throw error;
}

console.log('Disconnecting from Snowflake');
await snowflakeConnection.disconnect();

console.log('Exiting snowflake run');

process.exit()
}

run();