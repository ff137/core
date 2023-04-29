/**
 * Issues a request to the retriever to get GC (Game Coordinator) data for a match
 * Calls back with an object containing the GC data
 * */
import moment from "moment";
import { gunzipSync, gzipSync } from "zlib";
import { RETRIEVER_SECRET } from "../config";
import db from "../store/db";
import queries, { upsert } from "../store/queries";
import redis, { del, expireat, get, setex, zincrby } from "../store/redis";
import utility, { getRetrieverArr } from "../util/utility";

const secret = RETRIEVER_SECRET;
const { getData, redisCount } = utility;
const { insertMatch } = queries;

function handleGcData(match, body, cb) {
  // Persist parties and permanent buffs
  const players = body.match.players.map((p, i) => ({
    player_slot: p.player_slot,
    party_id: p.party_id?.low,
    permanent_buffs: p.permanent_buffs,
    party_size: body.match.players.filter(
      (matchPlayer) => matchPlayer.party_id?.low === p.party_id?.low
    ).length,
    net_worth: p.net_worth,
  }));
  const matchToInsert = {
    match_id: match.match_id,
    pgroup: match.pgroup,
    players,
    series_id: body.match.series_id,
    series_type: body.match.series_type,
  };
  const gcdata = {
    match_id: Number(match.match_id),
    cluster: body.match.cluster,
    replay_salt: body.match.replay_salt,
    series_id: body.match.series_id,
    series_type: body.match.series_type,
  };
  return insertMatch(
    matchToInsert,
    {
      type: "gcdata",
      skipParse: true,
    },
    (err) => {
      if (err) {
        return cb(err);
      }
      // Persist GC data to database
      return upsert(
        db,
        "match_gcdata",
        gcdata,
        {
          match_id: match.match_id,
        },
        (err) => {
          cb(err, gcdata);
        }
      );
    }
  );
}

del("nonRetryable");

function getGcDataFromRetriever(match, cb) {
  const retrieverArr = getRetrieverArr(match.useGcDataArr);
  // make array of retriever urls and use a random one on each retry
  let urls = retrieverArr.map(
    (r) => `http://${r}?key=${secret}&match_id=${match.match_id}`
  );
  return getData(
    { url: urls, noRetry: match.noRetry, timeout: 5000 },
    (err, body, metadata) => {
      if (
        err ||
        !body ||
        !body.match ||
        !body.match.replay_salt ||
        !body.match.players
      ) {
        // non-retryable error
        // redis.lpush('nonRetryable', JSON.stringify({ matchId: match.match_id, body }));
        // redis.ltrim('nonRetryable', 0, 10000);
        return cb(new Error("invalid body or error"));
      }
      // Count retriever calls
      redisCount(redis, "retriever");
      zincrby("retrieverCounts", 1, metadata.hostname);
      expireat(
        "retrieverCounts",
        moment().startOf("hour").add(1, "hour").format("X")
      );

      setex(
        `gcdata:${match.match_id}`,
        60 * 60,
        gzipSync(JSON.stringify(body))
      );
      // TODO add discovered account_ids to database and fetch account data/rank medal
      return handleGcData(match, body, cb);
    }
  );
}

export default function getGcData(match, cb) {
  const matchId = match.match_id;
  if (!matchId || Number.isNaN(Number(matchId)) || Number(matchId) <= 0) {
    return cb(new Error("invalid match_id"));
  }
  //return getGcDataFromRetriever(match, cb);
  return get(Buffer.from(`gcdata:${match.match_id}`), (err, body) => {
    if (err) {
      return cb(err);
    }
    if (body) {
      return handleGcData(match, JSON.parse(gunzipSync(body)), cb);
    }
    return getGcDataFromRetriever(match, cb);
  });
};
