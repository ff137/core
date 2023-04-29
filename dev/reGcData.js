/**
 * Call getGcData for all matches in match table
 * */
import { eachSeries } from "async";
import { select } from "../store/db";
import getGcData from "../util/getGcData";

select(["match_id"])
  .from("matches")
  .asCallback((err, matches) => {
    if (err) {
      throw err;
    }
    eachSeries(
      matches,
      (match, cb) => {
        console.log(match.match_id);
        getGcData(match, (err) => {
          if (err) {
            console.error(err);
          }
          cb();
        });
      },
      (err) => {
        if (err) {
          console.error(err);
        }
        process.exit(Number(err));
      }
    );
  });
