# snowflake-typescript-poc

## System Dependencies

### Node

- [Node.js](https://nodejs.org/) - v22.0.0+
- [npm](https://www.npmjs.com/) - v10.0.0+

## Running locally

In order to run this service locally, first copy the `.env.example` file and rename it to `env`. Fill out the information with appropriate values.

To start, make sure you have typescript installed on your machine

Finally, run `npm run start`

In order to interact with snowflake while running this script, you must create a connection first and then the other commands may be run.

### IMPORTANT

If a connection has been established, input the exit command (`X`) so that the connection can be safely disconnected. This prevents too many concurrent connections and rate limits