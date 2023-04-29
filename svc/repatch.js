/**
 * Periodically recalculate patch ID for matches in match table
 * */
import { eachSeries } from "async";
import { patch as _patch } from "dotaconstants";
import db, { select } from "../store/db";
import { upsert } from "../store/queries";
import utility, { getPatchIndex } from "../util/utility";

const { invokeInterval } = utility;

function rePatch() {
  select(["match_id", "start_time"])
    .from("matches")
    .asCallback((err, matchIds) => {
      if (err) {
        throw err;
      }
      eachSeries(
        matchIds,
        (match, cb) => {
          upsert(
            db,
            "match_patch",
            {
              match_id: match.match_id,
              patch:
                _patch[getPatchIndex(match.start_time)].name,
            },
            {
              match_id: match.match_id,
            },
            cb
          );
        },
        (err) => {
          console.error(err);
        }
      );
    });
}
invokeInterval(rePatch, 24 * 60 * 60 * 1000);
