// Computes rank/mmr distributions and stores in Redis
import fs from 'fs';
import async from 'async';
import constants from 'dotaconstants';
import db from '../store/db';
import redis from '../store/redis';
import { invokeIntervalAsync } from '../util/utility';

const sql: StringDict = {};
const sqlq = fs.readdirSync('./sql');
sqlq.forEach((f) => {
  sql[f.split('.')[0]] = fs.readFileSync(`./sql/${f}`, 'utf8');
});
function mapMmr(results: any) {
  const sum = results.rows.reduce(
    (prev: any, current: any) => ({
      count: prev.count + current.count,
    }),
    {
      count: 0,
    }
  );
  results.rows = results.rows.map((r: any, i: number) => {
    r.cumulative_sum = results.rows.slice(0, i + 1).reduce(
      (prev: any, current: any) => ({
        count: prev.count + current.count,
      }),
      {
        count: 0,
      }
    ).count;
    return r;
  });
  results.sum = sum;
  return results;
}
function mapCountry(results: any) {
  results.rows = results.rows.map((r: any) => {
    const ref = constants.countries[r.loccountrycode];
    r.common = ref ? ref.name.common : r.loccountrycode;
    return r;
  });
  return results;
}
function loadData(
  key: string,
  mapFunc: (result: { rows: any[] }) => any[],
  cb: NonUnknownErrorCb
) {
  db.raw(sql[key]).asCallback((err: any, results: any) => {
    if (err) {
      return cb(err);
    }
    return cb(err, mapFunc(results));
  });
}
async function doDistributions() {
  const result: any = await async.parallel({
    country_mmr(cb) {
      loadData('country_mmr', mapCountry, cb);
    },
    mmr(cb) {
      loadData('mmr', mapMmr, cb);
    },
    ranks(cb) {
      loadData('ranks', mapMmr, cb);
    },
  });
  Object.keys(result).forEach((key) => {
    redis.set(`distribution:${key}`, JSON.stringify(result[key]));
  });
}
invokeIntervalAsync(doDistributions, 6 * 60 * 60 * 1000);