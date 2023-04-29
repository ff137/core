import { MMSTATS_DATA_INTERVAL } from "../config";
import redis from "../store/redis";
import getMMStats from "../util/getMMStats";
import utility from "../util/utility";

const { invokeInterval } = utility;

function doMMStats(cb) {
  getMMStats(redis, cb);
}
invokeInterval(doMMStats, MMSTATS_DATA_INTERVAL * 60 * 1000); // Sample every 3 minutes
