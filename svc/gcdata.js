/**
 * Worker to fetch GC (Game Coordinator) data for matches
 * */
import { GCDATA_PARALLELISM } from "../config";
import { runQueue } from "../store/queue";
import getGcData from "../util/getGcData";
import utility from "../util/utility";

const { getRetrieverArr } = utility;
const retrieverArr = getRetrieverArr();

function processGcData(job, cb) {
  job.useGcDataArr = true;
  getGcData(job, cb);
}

runQueue(
  "gcQueue",
  Number(GCDATA_PARALLELISM) * retrieverArr.length,
  processGcData
);
