import SnowflakeConnection from "./connection";
import { logger } from "./logger";
import {fruits, fruitsRowToMerge} from "./test-data/fruits";

const promptSync = require('prompt-sync')({ sigint: true });

export const bulkInsertRowsIntoTable = async (connection: SnowflakeConnection, tableName: string, rows: any[][]): Promise<void> => {
    const query = `INSERT INTO ${tableName} (key, name, description, timestamp) VALUES (?, ?, ?, ?)`;

    const start = Date.now();
    await connection.executeQuery(query, rows);
    const end = Date.now();
    logger.success(`Inserted ${rows.length} rows into ${tableName} in ${end - start} ms`);
}

// Create function that converts the fruits array of objects to arrays
export const convertFruitsToArrays = (fruits: { key: any; name: string; description: string, timestamp: number }[]): any[][] => {
  return fruits.map(fruit => [fruit.key, fruit.name, fruit.description, new Date(fruit.timestamp).toISOString()]);
};

export const bulkMergeRowsIntoTable = async (connection: SnowflakeConnection, tableName: string, rows: any[][]): Promise<void> => {
  const query = `INSERT INTO ${tableName} (key, name, description, timestamp) VALUES (?, ?, ?, ?)`;

  const start = Date.now();
  await connection.executeQuery(query, rows);
  const end = Date.now();
  logger.success(`Inserted ${rows.length} rows into ${tableName} in ${end - start} ms`);
}

export const populateFruitsTable = async (connection: SnowflakeConnection, tableName: string): Promise<void> => {
  const query = `INSERT INTO ${tableName} (key, name, description, timestamp) VALUES (?, ?, ?, ?)`;
  const rows = convertFruitsToArrays(fruits);
  const promises = rows.map(row => {
    return connection.executeQuery(query, row);
  });
  const start = Date.now();
  await Promise.all(promises);
  const end = Date.now();
  logger.success(`Inserted ${rows.length} rows into ${tableName} in ${end - start} ms`);
}

export const populateFruitsTableBulk = async (connection: SnowflakeConnection, tableName: string): Promise<void> => {
  const rows = convertFruitsToArrays(fruits);
  await bulkInsertRowsIntoTable(connection, tableName, rows);
}


// Create snowflake function that deletes all rows from the table
export const deleteAllRowsFromTable = async (connection: SnowflakeConnection, tableName: string): Promise<void> => {
  const start = Date.now();
  const query = `DELETE FROM ${tableName}`;
  await connection.executeQuery(query);
  const end = Date.now();
  logger.success(`Deleted all rows from ${tableName} in ${end - start} ms`);
};

export const printInputCommands = (): any => {
  logger.input("\nInput commands:");
  logger.input('BP: Populate table with bulk insert');
  logger.input('P: Populate table');
  logger.input('G: Get all rows from table');
  logger.input('S: Singleton table merge POC');
  logger.input('BM: Bulk merge POC');
  logger.input('D: Delete all rows from table');
  logger.input('F: Filter out duplicate timestamps');
  logger.input('X: Exit\n');
  return promptSync('Enter snowflake command: ');
}

export const getAllTableRows = async (connection: SnowflakeConnection, tableName: string): Promise<any[]> => {
  const query = `SELECT * FROM ${tableName}`;
  const rows = await connection.executeQuery(query);

  return rows;
}

export const singletonTableMerge = async (connection: SnowflakeConnection, tableName: string): Promise<void> => {
  const singleton_table_merge_sql =
  `MERGE INTO ${tableName} AS target
    USING (
      SELECT ? AS key,
             ? AS name,
             ? AS description,
             ? AS timestamp
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
      UPDATE SET target.name = source.name,
                 target.description = source.description
    WHEN NOT MATCHED THEN
      INSERT (key, name, description, timestamp) VALUES (source.key, source.name, source.description, source.timestamp);`

  const rowsToMerge = convertFruitsToArrays(fruitsRowToMerge);

  logger.info(`Merging ${rowsToMerge.length} rows into ${tableName}...`);

  const start = Date.now();
  const promises = rowsToMerge.map(row => {
    return connection.executeQuery(singleton_table_merge_sql, row);
  });
  await Promise.all(promises);
  const end = Date.now();

  logger.success(`Merged 10 rows (5 inserts / 5 updates) individually in ${end - start} ms`)
}

export const pruneFruits = (): any => {
  const fruitsKeyMap = new Map<string, any>();

  fruitsRowToMerge.forEach((document) => {
    const key = `${document.key}`;
    const existingDocument = fruitsKeyMap.get(key);

    if (!existingDocument || existingDocument.timestamp < document.timestamp) {
      fruitsKeyMap.set(key, document);
    }
  });

  return Array.from(fruitsKeyMap.values());
};

export const bulkMerge = async (connection: SnowflakeConnection, tableName: string): Promise<void> => {
  // Creates another temporary table with the same columns as the original
  const table_create_sql = `CREATE OR REPLACE TEMP TABLE ${tableName}_staging LIKE ${tableName};`;

  const start = Date.now();
  await connection.executeQuery(table_create_sql);
  logger.info('Successfully created/replaced staging table');

  const prunedFruits = pruneFruits();
  const rows = convertFruitsToArrays(prunedFruits);
  await bulkInsertRowsIntoTable(connection, `${tableName}_staging`, rows);
  logger.info('Successfully inserted rows into staging table');
  const merge_sql =
  `MERGE INTO ${tableName} AS target
    USING ${tableName}_staging AS source
    ON target.key = source.key
          WHEN MATCHED THEN
            UPDATE SET target.name = source.name,
                       target.description = source.description,
                       target.timestamp = source.timestamp
          WHEN NOT MATCHED THEN
            INSERT (key, name, description, timestamp) VALUES (source.key, source.name, source.description, source.timestamp);`;
  await connection.executeQuery(merge_sql);
  const end = Date.now();
  logger.success(`Merged ${rows.length} rows into ${tableName} in ${end - start} ms`);
}