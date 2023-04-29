import { eachLimit } from "async";
import { keys as _keys, lrange } from "../store/redis";
import { transaction, raw } from "../store/db";
import { average } from "../util/utility";

transaction((trx) => {
  _keys("mmr_estimates:*", (err, keys) => {
    eachLimit(
      keys,
      1000,
      (key, cb) => {
        console.log(key);
        lrange(key, 0, -1, (err, result) => {
          const accountId = key.split(":")[1];
          const data = result.filter((d) => d).map((d) => Number(d));
          const estimate = average(data);
          if (accountId && estimate) {
            raw(
              "INSERT INTO mmr_estimates VALUES (?, ?) ON CONFLICT(account_id) DO UPDATE SET estimate = ?",
              [accountId, estimate, estimate]
            ).asCallback(cb);
          } else {
            cb();
          }
        });
      },
      (err) => {
        if (err) {
          return trx.rollback(err);
        }
        return trx.commit();
      }
    );
  });
});
