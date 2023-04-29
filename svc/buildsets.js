import buildSets from "../store/buildSets";
import redis from "../store/redis";
import db from "../store/db";
import utility from "../util/utility";

const { invokeInterval } = utility;

function doBuildSets(cb) {
  buildSets(db, redis, cb);
}
invokeInterval(doBuildSets, 60 * 1000);
