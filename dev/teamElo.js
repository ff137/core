/**
 * Computes team Elo ratings by game
 * */
import { parse } from "JSONStream";
import { eachSeries } from "async";
import { raw } from "../store/db";
// Keep each team's rating in memory and update
const teams = {};
const wins = {};
const losses = {};
const startTimes = {};
const kFactor = 32;
// Read a stream from the database
const stream = raw(
    `
SELECT team_match.team_id team_id1, tm2.team_id team_id2, matches.match_id, team_match.radiant = radiant_win team1_win, start_time
FROM team_match
JOIN matches using(match_id)
JOIN team_match tm2 on team_match.match_id = tm2.match_id AND team_match.team_id < tm2.team_id
WHERE matches.radiant_team_id IS NOT NULL AND matches.dire_team_id IS NOT NULL
ORDER BY match_id ASC
`
  )
  .stream();
stream.pipe(parse());
stream.on("data", (match) => {
  // console.log(JSON.stringify(match));
  if (!teams[match.team_id1]) {
    teams[match.team_id1] = 1000;
  }
  if (!teams[match.team_id2]) {
    teams[match.team_id2] = 1000;
  }
  if (!wins[match.team_id1]) {
    wins[match.team_id1] = 0;
  }
  if (!wins[match.team_id2]) {
    wins[match.team_id2] = 0;
  }
  if (!losses[match.team_id1]) {
    losses[match.team_id1] = 0;
  }
  if (!losses[match.team_id2]) {
    losses[match.team_id2] = 0;
  }
  startTimes[match.team_id1] = match.start_time;
  startTimes[match.team_id2] = match.start_time;
  const currRating1 = teams[match.team_id1];
  const currRating2 = teams[match.team_id2];
  const r1 = 10 ** (currRating1 / 400);
  const r2 = 10 ** (currRating2 / 400);
  const e1 = r1 / (r1 + r2);
  const e2 = r2 / (r1 + r2);
  const win1 = Number(match.team1_win);
  const win2 = Number(!win1);
  const ratingDiff1 = kFactor * (win1 - e1);
  const ratingDiff2 = kFactor * (win2 - e2);
  teams[match.team_id1] += ratingDiff1;
  teams[match.team_id2] += ratingDiff2;
  wins[match.team_id1] += win1;
  wins[match.team_id2] += win2;
  losses[match.team_id1] += Number(!win1);
  losses[match.team_id2] += Number(!win2);
});
stream.on("end", () => {
  console.log(teams, wins, losses, startTimes);
  // Write the results to table
  eachSeries(
    Object.keys(teams),
    (teamId, cb) => {
      console.log([
        teamId,
        teams[teamId],
        wins[teamId],
        losses[teamId],
        startTimes[teamId],
      ]);
      raw(
        `INSERT INTO team_rating(team_id, rating, wins, losses, last_match_time) VALUES(?, ?, ?, ?, ?)
  ON CONFLICT(team_id) DO UPDATE SET team_id=EXCLUDED.team_id, rating=EXCLUDED.rating, wins=EXCLUDED.wins, losses=EXCLUDED.losses, last_match_time=EXCLUDED.last_match_time`,
        [
          teamId,
          teams[teamId],
          wins[teamId],
          losses[teamId],
          startTimes[teamId],
        ]
      ).asCallback(cb);
    },
    (err) => {
      if (err) {
        console.error(err);
      }
      process.exit(Number(err));
    }
  );
});
stream.on("error", (err) => {
  throw err;
});
