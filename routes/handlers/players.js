const async = require("async");
const cacheFunctions = require("../../store/cacheFunctions");
const db = require("../../store/db");
const queries = require("../../store/queries");
const utility = require("../../util/utility");

async function getPlayersByRank(req, res, cb) {
  try {
    const result = await db.raw(
      `
      SELECT account_id, rating, fh_unavailable
      FROM players
      JOIN rank_tier
      USING (account_id)
      ORDER BY rating DESC
      LIMIT 100
      `,
      []
    );
    return res.json(result.rows);
  } catch (err) {
    return cb(err);
  }
}

async function getPlayersByAccountId(req, res, cb) {
    const accountId = Number(req.params.account_id);
    async.parallel(
    {
        profile(cb) {
        queries.getPlayer(db, accountId, (err, playerData) => {
            if (playerData !== null && playerData !== undefined) {
            playerData.is_contributor = utility.isContributor(accountId);
            playerData.is_subscriber = Boolean(playerData?.status);
            }
            cb(err, playerData);
        });
        },
        solo_competitive_rank(cb) {
        db.first()
            .from("solo_competitive_rank")
            .where({ account_id: accountId })
            .asCallback((err, row) => {
            cb(err, row ? row.rating : null);
            });
        },
        competitive_rank(cb) {
        db.first()
            .from("competitive_rank")
            .where({ account_id: accountId })
            .asCallback((err, row) => {
            cb(err, row ? row.rating : null);
            });
        },
        rank_tier(cb) {
        db.first()
            .from("rank_tier")
            .where({ account_id: accountId })
            .asCallback((err, row) => {
            cb(err, row ? row.rating : null);
            });
        },
        leaderboard_rank(cb) {
        db.first()
            .from("leaderboard_rank")
            .where({ account_id: accountId })
            .asCallback((err, row) => {
            cb(err, row ? row.rating : null);
            });
        },
        mmr_estimate(cb) {
        queries.getMmrEstimate(accountId, (err, est) =>
            cb(err, est || {})
        );
        },
    },
    (err, result) => {
        if (err) {
        return cb(err);
        }
        return res.json(result);
    }
    );
}

async function getPlayersByAccountIdWl(req, res, cb) {
  const result = {
    win: 0,
    lose: 0,
  };
  req.queryObj.project = req.queryObj.project.concat(
    "player_slot",
    "radiant_win"
  );
  queries.getPlayerMatches(
    req.params.account_id,
    req.queryObj,
    (err, cache) => {
      if (err) {
        return cb(err);
      }
      cache.forEach((m) => {
        if (utility.isRadiant(m) === m.radiant_win) {
          result.win += 1;
        } else {
          result.lose += 1;
        }
      });
      return cacheFunctions.sendDataWithCache(req, res, result, "wl");
    }
  );
}

async function getPlayersByAccountIdRecentMatches(req, res, cb) {
  queries.getPlayerMatches(
    req.params.account_id,
    {
      project: req.queryObj.project.concat([
        "hero_id",
        "start_time",
        "duration",
        "player_slot",
        "radiant_win",
        "game_mode",
        "lobby_type",
        "version",
        "kills",
        "deaths",
        "assists",
        "skill",
        "average_rank",
        "xp_per_min",
        "gold_per_min",
        "hero_damage",
        "tower_damage",
        "hero_healing",
        "last_hits",
        "lane",
        "lane_role",
        "is_roaming",
        "cluster",
        "leaver_status",
        "party_size",
      ]),
      dbLimit: 20,
    },
    (err, cache) => {
      if (err) {
        return cb(err);
      }
      return res.json(cache.filter((match) => match.duration));
    }
  );
}

async function getPlayersByAccountIdMatches(req, res, cb) {
  // Use passed fields as additional fields, if available
  const additionalFields = req.query.project || [
    "hero_id",
    "start_time",
    "duration",
    "player_slot",
    "radiant_win",
    "game_mode",
    "lobby_type",
    "version",
    "kills",
    "deaths",
    "assists",
    "skill",
    "average_rank",
    "leaver_status",
    "party_size",
  ];
  req.queryObj.project = req.queryObj.project.concat(additionalFields);
  queries.getPlayerMatches(
    req.params.account_id,
    req.queryObj,
    (err, cache) => {
      if (err) {
        return cb(err);
      }
      return res.json(cache);
    }
  );
}

async function getPlayersByAccountIdHeroes(req, res, cb) {
  const heroes = {};
  // prefill heroes with every hero
  Object.keys(constants.heroes).forEach((heroId) => {
    hero_id_int = parseInt(heroId);
    const hero = {
      hero_id: hero_id_int,
      last_played: 0,
      games: 0,
      win: 0,
      with_games: 0,
      with_win: 0,
      against_games: 0,
      against_win: 0,
    };
    heroes[hero_id_int] = hero;
  });
  req.queryObj.project = req.queryObj.project.concat(
    "heroes",
    "account_id",
    "start_time",
    "player_slot",
    "radiant_win"
  );
  queries.getPlayerMatches(
    req.params.account_id,
    req.queryObj,
    (err, cache) => {
      if (err) {
        return cb(err);
      }
      cache.forEach((m) => {
        const { isRadiant } = utility;
        const playerWin = isRadiant(m) === m.radiant_win;
        const group = m.heroes || {};
        Object.keys(group).forEach((key) => {
          const tm = group[key];
          const tmHero = tm.hero_id;
          // don't count invalid heroes
          if (tmHero in heroes) {
            if (isRadiant(tm) === isRadiant(m)) {
              if (tm.account_id === m.account_id) {
                heroes[tmHero].games += 1;
                heroes[tmHero].win += playerWin ? 1 : 0;
                if (m.start_time > heroes[tmHero].last_played) {
                  heroes[tmHero].last_played = m.start_time;
                }
              } else {
                heroes[tmHero].with_games += 1;
                heroes[tmHero].with_win += playerWin ? 1 : 0;
              }
            } else {
              heroes[tmHero].against_games += 1;
              heroes[tmHero].against_win += playerWin ? 1 : 0;
            }
          }
        });
      });
      const result = Object.keys(heroes)
        .map((k) => heroes[k])
        .filter(
          (hero) =>
            !req.queryObj.having ||
            hero.games >= Number(req.queryObj.having)
        )
        .sort((a, b) => b.games - a.games);
      return cacheFunctions.sendDataWithCache(
        req,
        res,
        result,
        "heroes"
      );
    }
  );
}

async function getPlayersByAccountIdPeers(req, res, cb) {
  req.queryObj.project = req.queryObj.project.concat(
    "heroes",
    "start_time",
    "player_slot",
    "radiant_win",
    "gold_per_min",
    "xp_per_min"
  );
  queries.getPlayerMatches(
    req.params.account_id,
    req.queryObj,
    (err, cache) => {
      if (err) {
        return cb(err);
      }
      const teammates = utility.countPeers(cache);
      return queries.getPeers(
        db,
        teammates,
        {
          account_id: req.params.account_id,
        },
        (err, result) => {
          if (err) {
            return cb(err);
          }
          return cacheFunctions.sendDataWithCache(
            req,
            res,
            result,
            "peers"
          );
        }
      );
    }
  );
}

module.exports = {
  getPlayersByRank,
  getPlayersByAccountId,
  getPlayersByAccountIdWl,
};
