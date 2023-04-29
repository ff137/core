/**
 * Recalculate patch ID for matches in match table
 * */
import { eachSeries } from "async";
import { patch as _patch } from "dotaconstants";
import db, { select } from "../store/db";
import { upsert } from "../store/queries";
import { getPatchIndex } from "../util/utility";

select(["match_id", "start_time"])
  .from("matches")
  .orderBy("match_id", "desc")
  .asCallback((err, matchIds) => {
    if (err) {
      throw err;
    }
    eachSeries(
      matchIds,
      (match, cb) => {
        const patch =
          _patch[getPatchIndex(match.start_time)].name;
        console.log(match.match_id, patch);
        upsert(
          db,
          "match_patch",
          {
            match_id: match.match_id,
            patch,
          },
          {
            match_id: match.match_id,
          },
          cb
        );
      },
      (err) => {
        process.exit(Number(err));
      }
    );
  });
