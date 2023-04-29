import { eachSeries } from "async";
import db from "../store/db";
import { upsert } from "../store/queries";
import utility, { generateJob, getData } from "../util/utility";

const { invokeInterval } = utility;

function doItems(cb) {
  const container = generateJob("api_items", {
    language: "english",
  });
  getData(container.url, (err, body) => {
    if (err) {
      return cb(err);
    }
    if (!body || !body.result || !body.result.items) {
      return cb();
    }
    return eachSeries(
      body.result.items,
      (item, cb) => {
        upsert(
          db,
          "items",
          item,
          {
            id: item.id,
          },
          cb
        );
      },
      cb
    );
  });
}
invokeInterval(doItems, 60 * 60 * 1000);
