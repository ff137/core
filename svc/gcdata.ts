// Processes a queue of requests for gcdata (replay salts) without parsing
// The parser will also request gcdata if needed
import { getOrFetchGcData } from '../store/getGcData';
import queue from '../store/queue';
import config from '../config.js';
import { getRetrieverCount } from '../util/utility';

async function processGcData(job: GcDataJob) {
  // We don't need the result, but we do want to respect the DISABLE_REGCDATA setting
  await getOrFetchGcData(job.match_id, job.pgroup, job.noRetry);
  await new Promise(resolve => setTimeout(resolve, 500));
}

console.log('[GCDATA] starting');
queue.runQueue(
  'gcQueue',
  Number(config.GCDATA_PARALLELISM) * getRetrieverCount(),
  processGcData,
);
