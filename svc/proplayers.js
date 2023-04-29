import { each } from "async";
import db from "../store/db";
import { upsert } from "../store/queries";
import utility from "../util/utility";

const { invokeInterval, generateJob, getData } = utility;

function doProPlayers(cb) {
  const container = generateJob("api_notable", {});
  getData(container.url, (err, body) => {
    if (err) {
      return cb(err);
    }
    return each(
      body.player_infos,
      (p, cb) => {
        upsert(
          db,
          "notable_players",
          p,
          {
            account_id: p.account_id,
          },
          cb
        );
      },
      cb
    );
  });
}
invokeInterval(doProPlayers, 30 * 60 * 1000);
