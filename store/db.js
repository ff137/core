/**
 * Interface to PostgreSQL client
 * */
import knex from "knex";
import { types } from "pg";
import { POSTGRES_URL } from "../config";

// remember: all values returned from the server are either NULL or a string
types.setTypeParser(20, (val) => (val === null ? null : parseInt(val, 10)));
console.log("connecting %s", POSTGRES_URL);
const db = knex({
  client: "pg",
  connection: POSTGRES_URL,
  pool: {
    // min: 2,
    // max: 20,
    // afterCreate: (conn, done) => {
    //   // Set the minimum similarity for pg_trgm
    //   conn.query('SELECT set_limit(0.6);', (err) => {
    //     // if err is not falsy, connection is discarded from pool
    //     done(err, conn);
    //   });
    // },
  },
});
// db.on('query-error', (err) => {
//   console.error(err);
// });
export default db;
