/**
 * Worker checking the GetMatchHistory endpoint to get skill data for matches
 * */
import { eachLimit, eachSeries } from "async";
import { heroes as _heroes } from "dotaconstants";
import { STEAM_API_KEY } from "../config";
import { generateJob, getData } from "../util/utility";

const apiKeys = STEAM_API_KEY.split(",");
const parallelism = Math.min(3, apiKeys.length);
const skills = [1, 2, 3];
const heroes = Object.keys(_heroes);
const permute = [];

function getPageData(start, options, cb) {
  const container = generateJob("api_skill", {
    skill: options.skill,
    hero_id: options.hero_id,
    start_at_match_id: start,
  });
  getData(
    {
      url: container.url,
    },
    (err, data) => {
      if (err) {
        return cb(err);
      }
      if (!data || !data.result || !data.result.matches) {
        return getPageData(start, options, cb);
      }
      // data is in data.result.matches
      const { matches } = data.result;
      return eachSeries(
        matches,
        (m, cb) => {
          cb();
          // insertMatchSkillCassandra({
          //   match_id: m.match_id,
          //   skill: options.skill,
          //   players: m.players,
          // }, cb);
        },
        (err) => {
          if (err) {
            return cb(err);
          }
          // repeat until results_remaining===0
          if (data.result.results_remaining === 0) {
            return cb(err);
          }
          const nextStart = matches[matches.length - 1].match_id - 1;
          return getPageData(nextStart, options, cb);
        }
      );
    }
  );
}

function scanSkill() {
  eachLimit(
    permute,
    parallelism,
    (object, cb) => {
      // use api_skill
      const start = null;
      getPageData(start, object, cb);
    },
    (err) => {
      if (err) {
        throw err;
      }
      return scanSkill();
    }
  );
}

for (let i = 0; i < heroes.length; i += 1) {
  for (let j = 0; j < skills.length; j += 1) {
    permute.push({
      skill: skills[j],
      hero_id: heroes[i],
    });
  }
}
// permute = [{skill:1,hero_id:1}];
console.log(permute.length);
scanSkill();
