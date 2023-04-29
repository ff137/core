/**
 * Methods for search functionality
 * */
import { parallel } from "async";
import db from "./db";
/**
 * @param db - database object
 * @param search - object for where parameter of query
 * @param cb - callback
 */
function findPlayer(search, cb) {
  db.first(["account_id", "personaname", "avatarfull"])
    .from("players")
    .where(search)
    .asCallback(cb);
}

function search(options, cb) {
  const query = options.q;
  parallel(
    {
      account_id(callback) {
        if (Number.isNaN(Number(query))) {
          return callback();
        }
        return findPlayer(
          {
            account_id: Number(query),
          },
          callback
        );
      },
      personaname(callback) {
        db.raw(
          `
        SELECT * FROM 
        (SELECT account_id, avatarfull, personaname, last_match_time
        FROM players 
        WHERE personaname ILIKE ?
        LIMIT 100) search
        ORDER BY last_match_time DESC NULLS LAST;
        `,
          [`%${query}%`]
        ).asCallback((err, result) => {
          if (err) {
            return callback(err);
          }
          return callback(err, result.rows);
        });
      },
    },
    (err, result) => {
      if (err) {
        return cb(err);
      }
      let ret = [];
      Object.keys(result).forEach((key) => {
        if (result[key]) {
          ret = ret.concat(result[key]);
        }
      });
      return cb(null, ret);
    }
  );
}
export default search;
