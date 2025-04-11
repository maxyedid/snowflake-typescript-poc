import {createConnection, Connection, ConnectionOptions} from 'snowflake-sdk';

export default class SnowflakeConnection {
    private connection: Connection;

    constructor(options: ConnectionOptions) {
        this.connection = createConnection(options);
    }

    connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.connection.connectAsync((err, conn) => {
                if (err) {
                    reject(err);
                } else {
                    console.log('Connected to Snowflake');
                    resolve();
                }
            });
        });
    }

    disconnect(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.connection.destroy((err) => {
                if (err) {
                    reject(err);
                } else {
                    console.log('Disconnected from Snowflake');
                    resolve();
                }
            });
        });
    }

    executeQuery(query: string, binds?: any[]): Promise<any> {
        return new Promise((resolve, reject) => {
            this.connection.execute({
                sqlText: query,
                binds,
                complete: (err, stmt, rows) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(rows);
                    }
                }
            });
        });
    }
}