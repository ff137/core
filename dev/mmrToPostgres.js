import { eachSeries } from "async";
import { zrange } from "../store/redis";
import { raw } from "../store/db";

zrange("solo_competitive_rank", 0, -1, "WITHSCORES", (err, ids) => {
  const inserts = [];
  for (let i = 0; i < ids.length; i += 2) {
    inserts.push({ account_id: ids[i], rating: ids[i + 1] });
  }
  eachSeries(inserts, (ins, cb) => {
    console.log(ins);
    raw(
      "INSERT INTO solo_competitive_rank(account_id, rating) VALUES (?, ?) ON CONFLICT(account_id) DO NOTHING",
      [ins.account_id, ins.rating]
    ).asCallback(cb);
  });
});

zrange("competitive_rank", 0, -1, "WITHSCORES", (err, ids) => {
  const inserts = [];
  for (let i = 0; i < ids.length; i += 2) {
    inserts.push({ account_id: ids[i], rating: ids[i + 1] });
  }
  eachSeries(inserts, (ins, cb) => {
    console.log(ins);
    raw(
      "INSERT INTO competitive_rank(account_id, rating) VALUES (?, ?) ON CONFLICT(account_id) DO NOTHING",
      [ins.account_id, ins.rating]
    ).asCallback(cb);
  });
});
