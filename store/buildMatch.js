/**
 * Functions to build/cache match object
 * */
import { heroes } from "dotaconstants";
import { promisify } from "util";
import { ENABLE_MATCH_CACHE, MATCH_CACHE_SECONDS } from "../config";
import { execute } from "../store/cassandra";
import { first, raw } from "../store/db";
import redis, { get, setex } from "../store/redis";
import compute from "../util/compute";
import utility, { generateJob, getData, redisCount } from "../util/utility";
import { getMatchBenchmarksPromisified, insertMatch } from "./queries";

const { computeMatchData } = compute;
const { deserialize, buildReplayUrl, isContributor } = utility;
const getRedisAsync = promisify(get).bind(redis);

async function getMatchData(matchId) {
  const result = await execute(
    "SELECT * FROM matches where match_id = ?",
    [Number(matchId)],
    {
      prepare: true,
      fetchSize: 1,
      autoPage: true,
    }
  );
  const deserializedResult = result.rows.map((m) => deserialize(m));
  return Promise.resolve(deserializedResult[0]);
}

async function getPlayerMatchData(matchId) {
  const result = await execute(
    "SELECT * FROM player_matches where match_id = ?",
    [Number(matchId)],
    {
      prepare: true,
      fetchSize: 24,
      autoPage: true,
    }
  );
  const deserializedResult = result.rows.map((m) => deserialize(m));
  return Promise.all(
    deserializedResult.map((r) =>
      raw(
          `
        SELECT personaname, name, last_login 
        FROM players
        LEFT JOIN notable_players
        ON players.account_id = notable_players.account_id
        WHERE players.account_id = ?
      `,
          [r.account_id]
        )
        .then((names) => ({ ...r, ...names.rows[0] }))
    )
  );
}

async function extendPlayerData(player, match) {
  const p = {
    ...player,
    radiant_win: match.radiant_win,
    start_time: match.start_time,
    duration: match.duration,
    cluster: match.cluster,
    lobby_type: match.lobby_type,
    game_mode: match.game_mode,
    is_contributor: isContributor(player.account_id),
  };
  computeMatchData(p);
  const row = await first()
    .from("rank_tier")
    .where({ account_id: p.account_id || null });
  p.rank_tier = row ? row.rating : null;
  const subscriber = await first()
    .from("subscriber")
    .where({ account_id: p.account_id || null });
  p.is_subscriber = Boolean(subscriber?.status);
  return Promise.resolve(p);
}

async function prodataInfo(matchId) {
  const result = await first(["radiant_team_id", "dire_team_id", "leagueid"])
    .from("matches")
    .where({
      match_id: matchId,
    });
  if (!result) {
    return Promise.resolve({});
  }
  const leaguePromise = first().from("leagues").where({
    leagueid: result.leagueid,
  });
  const radiantTeamPromise = first().from("teams").where({
    team_id: result.radiant_team_id,
  });
  const direTeamPromise = first().from("teams").where({
    team_id: result.dire_team_id,
  });
  const [league, radiantTeam, direTeam] = await Promise.all([
    leaguePromise,
    radiantTeamPromise,
    direTeamPromise,
  ]);
  return Promise.resolve({
    league,
    radiant_team: radiantTeam,
    dire_team: direTeam,
  });
}

async function getMatch(matchId) {
  if (!matchId || Number.isNaN(Number(matchId)) || Number(matchId) <= 0) {
    return Promise.resolve();
  }
  const match = await getMatchData(matchId);
  if (!match) {
    return Promise.resolve();
  }
  redisCount(redis, "build_match");
  let playersMatchData = [];
  try {
    playersMatchData = await getPlayerMatchData(matchId);
    if (playersMatchData.length === 0) {
      throw new Error("no players found for match");
    }
  } catch (e) {
    console.error(e);
    if (
      e.message.startsWith("Server failure during read query") ||
      e.message.startsWith("no players found") ||
      e.message.startsWith("Unexpected") ||
      e.message.includes("Attempt to access memory outside buffer bounds")
    ) {
      // Delete and request new
      await execute(
        "DELETE FROM player_matches where match_id = ?",
        [Number(matchId)],
        { prepare: true }
      );
      const match = {
        match_id: Number(matchId),
      };
      await new Promise((resolve, reject) => {
        getData(
          generateJob("api_details", match).url,
          (err, body) => {
            if (err) {
              console.error(err);
              return reject();
            }
            // match details response
            const match = body.result;
            return insertMatch(
              match,
              {
                type: "api",
                skipParse: true,
              },
              () => {
                // Count for logging
                redisCount(redis, "cassandra_repair");
                resolve();
              }
            );
          }
        );
      });
      playersMatchData = await getPlayerMatchData(matchId);
    } else {
      throw e;
    }
  }
  const playersPromise = Promise.all(
    playersMatchData.map((p) => extendPlayerData(p, match))
  );
  const gcdataPromise = first().from("match_gcdata").where({
    match_id: matchId,
  });
  const cosmeticsPromise = Promise.all(
    Object.keys(match.cosmetics || {}).map((itemId) =>
      first().from("cosmetics").where({
        item_id: itemId,
      })
    )
  );
  const prodataPromise = prodataInfo(matchId);

  const [players, gcdata, prodata, cosmetics] = await Promise.all([
    playersPromise,
    gcdataPromise,
    prodataPromise,
    cosmeticsPromise,
  ]);

  let matchResult = {
    ...match,
    ...gcdata,
    ...prodata,
    players,
  };

  if (cosmetics) {
    const playersWithCosmetics = matchResult.players.map((p) => {
      const hero = heroes[p.hero_id] || {};
      const playerCosmetics = cosmetics
        .filter(Boolean)
        .filter(
          (c) =>
            match.cosmetics[c.item_id] === p.player_slot &&
            (!c.used_by_heroes || c.used_by_heroes === hero.name)
        );
      return {
        ...p,
        cosmetics: playerCosmetics,
      };
    });
    matchResult = {
      ...matchResult,
      players: playersWithCosmetics,
    };
  }
  computeMatchData(matchResult);
  if (matchResult.replay_salt) {
    matchResult.replay_url = buildReplayUrl(
      matchResult.match_id,
      matchResult.cluster,
      matchResult.replay_salt
    );
  }
  const matchWithBenchmarks = await getMatchBenchmarksPromisified(
    matchResult
  );
  return Promise.resolve(matchWithBenchmarks);
}

async function buildMatch(matchId) {
  const key = `match:${matchId}`;
  const reply = await getRedisAsync(key);
  if (reply) {
    return Promise.resolve(JSON.parse(reply));
  }
  const match = await getMatch(matchId);
  if (!match) {
    return Promise.resolve();
  }
  if (match.version && ENABLE_MATCH_CACHE) {
    await setex(key, MATCH_CACHE_SECONDS, JSON.stringify(match));
  }
  return Promise.resolve(match);
}

export default buildMatch;
