// Processes a queue of requests for gcdata (replay salts) without parsing
// The parser will also request gcdata if needed
import { ensureGcData } from '../store/getGcData';
import queue from '../store/queue';
import config from '../config.js';
import { getRetrieverArr } from '../util/utility';

const retrieverArr = getRetrieverArr();

async function processGcData(job: GcDataJob) {
  job.useGcDataArr = true;
  await ensureGcData(job);
}

console.log('[GCDATA] starting');
queue.runQueue(
  'gcQueue',
  Number(config.GCDATA_PARALLELISM) * retrieverArr.length,
  processGcData
);