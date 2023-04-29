import { parallel } from "async";
import { countries } from "dotaconstants";
import { readdirSync, readFileSync } from "fs";
import { raw } from "../store/db";
import { set } from "../store/redis";
import utility from "../util/utility";

const { invokeInterval } = utility;

const sql = {};
const sqlq = readdirSync("./sql");
sqlq.forEach((f) => {
  sql[f.split(".")[0]] = readFileSync(`./sql/${f}`, "utf8");
});

function mapMmr(results) {
  const sum = results.rows.reduce(
    (prev, current) => ({
      count: prev.count + current.count,
    }),
    {
      count: 0,
    }
  );
  results.rows = results.rows.map((r, i) => {
    r.cumulative_sum = results.rows.slice(0, i + 1).reduce(
      (prev, current) => ({
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

function mapCountry(results) {
  results.rows = results.rows.map((r) => {
    const ref = countries[r.loccountrycode];
    r.common = ref ? ref.name.common : r.loccountrycode;
    return r;
  });
  return results;
}

function loadData(key, mapFunc, cb) {
  raw(sql[key]).asCallback((err, results) => {
    if (err) {
      return cb(err);
    }
    return cb(err, mapFunc(results));
  });
}

function doDistributions(cb) {
  parallel(
    {
      country_mmr(cb) {
        loadData("country_mmr", mapCountry, cb);
      },
      mmr(cb) {
        loadData("mmr", mapMmr, cb);
      },
      ranks(cb) {
        loadData("ranks", mapMmr, cb);
      },
    },
    (err, result) => {
      if (err) {
        return cb(err);
      }
      Object.keys(result).forEach((key) => {
        set(`distribution:${key}`, JSON.stringify(result[key]));
      });
      return cb(err);
    }
  );
}
invokeInterval(doDistributions, 6 * 60 * 60 * 1000);
