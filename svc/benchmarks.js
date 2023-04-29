import { BENCHMARK_RETENTION_MINUTES } from "../config";
import buildMatch from "../store/buildMatch";
import { runQueue } from "../store/queue";
import { expireat, zadd } from "../store/redis";
import benchmarksUtil from "../util/benchmarksUtil";
import { getStartOfBlockMinutes, isSignificant } from "../util/utility";

const { benchmarks } = benchmarksUtil;

async function doParsedBenchmarks(matchID, cb) {
  try {
    const match = await buildMatch(matchID);
    if (match.players && isSignificant(match)) {
      for (let i = 0; i < match.players.length; i += 1) {
        const p = match.players[i];
        // only do if all players have heroes
        if (p.hero_id) {
          Object.keys(benchmarks).forEach((key) => {
            const metric = benchmarks[key](match, p);
            if (
              metric !== undefined &&
              metric !== null &&
              !Number.isNaN(Number(metric))
            ) {
              const rkey = [
                "benchmarks",
                getStartOfBlockMinutes(
                  BENCHMARK_RETENTION_MINUTES,
                  0
                ),
                key,
                p.hero_id,
              ].join(":");
              zadd(rkey, metric, match.match_id);
              // expire at time two epochs later (after prev/current cycle)
              const expiretime = getStartOfBlockMinutes(
                BENCHMARK_RETENTION_MINUTES,
                2
              );
              expireat(rkey, expiretime);
            }
          });
        }
      }
      return cb();
    }
    return cb();
  } catch (err) {
    return cb(err);
  }
}

runQueue("parsedBenchmarksQueue", 1, doParsedBenchmarks);
