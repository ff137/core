/**
 * Worker that parses replays
 * The actual parsing is done by invoking the Java-based parser.
 * This produces an event stream (newline-delimited JSON)
 * Stream is run through a series of processors to count/aggregate it into a single object
 * This object is passed to insertMatch to persist the data into the database.
 * */
import { series } from "async";
import { exec } from "child_process";
import express from "express";
import { BENCHMARKS_SAMPLE_PERCENT, NODE_ENV, PARSER_HOST, PARSER_PARALLELISM, PARSER_PORT, PORT, SCENARIOS_SAMPLE_PERCENT } from "../config";
import queries from "../store/queries";
import { runReliableQueue } from "../store/queue";
import getGcData from "../util/getGcData";
import utility from "../util/utility";
const numCPUs = require("os").cpus().length;

const { insertMatch } = queries;
const { buildReplayUrl } = utility;

const app = express();
app.get("/healthz", (req, res) => {
  res.end("ok");
});
app.listen(PORT || PARSER_PORT);

function runParse(match, job, cb) {
  let { url } = match;
  if (NODE_ENV === "test") {
    url = `https://odota.github.io/testfiles/${match.match_id}_1.dem`;
  }
  console.log(new Date(), url);
  exec(
    `curl --max-time 180 --fail ${url} | ${
      url && url.slice(-3) === "bz2" ? "bunzip2" : "cat"
    } | curl -X POST -T - ${
      PARSER_HOST
    } | node processors/createParsedDataBlob.js ${match.match_id} ${Boolean(
      match.doLogParse
    )}`,
    { shell: true, maxBuffer: 10 * 1024 * 1024 },
    (err, stdout) => {
      if (err) {
        return cb(err);
      }
      const result = { ...JSON.parse(stdout), ...match };
      return insertMatch(
        result,
        {
          type: "parsed",
          skipParse: true,
          doLogParse: match.doLogParse,
          doScenarios:
            match.origin === "scanner" &&
            match.match_id % 100 < SCENARIOS_SAMPLE_PERCENT,
          doParsedBenchmarks:
            match.origin === "scanner" &&
            match.match_id % 100 < BENCHMARKS_SAMPLE_PERCENT,
          doTellFeed: match.origin === "scanner",
        },
        cb
      );
    }
  );
}

function parseProcessor(job, cb) {
  const match = job;
  if (!match.game_mode && match.origin !== "scanner") {
    // Skip parses without game_mode that weren't from scanner (do this to clear queue of event matches)
    return cb();
  }
  series(
    {
      getDataSource(cb) {
        getGcData(match, (err, result) => {
          if (err) {
            return cb(err);
          }
          match.url = buildReplayUrl(
            result.match_id,
            result.cluster,
            result.replay_salt
          );
          return cb(err);
        });
      },
      runParse(cb) {
        runParse(match, job, cb);
      },
    },
    (err) => {
      if (err) {
        console.error(err.stack || err);
      } else {
        console.log("completed parse of match %s", match.match_id);
      }
      return cb(err, match.match_id);
    }
  );
}

runReliableQueue(
  "parse",
  Number(PARSER_PARALLELISM) || numCPUs,
  parseProcessor
);
