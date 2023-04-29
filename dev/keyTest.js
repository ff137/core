import request from "request";
import { eachSeries } from "async";
import { STEAM_API_KEY } from "../config";

const output = [];

eachSeries(
  STEAM_API_KEY.split(","),
  (key, cb) => {
    setTimeout(() => {
      request(
        `http://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/?key=${key}`,
        (err, resp, body) => {
          console.log(key, resp.statusCode);
          if (resp.statusCode !== 200) {
            console.log(body);
          } else {
            output.push(key);
          }
          cb();
        }
      );
    }, 1000);
  },
  () => {
    console.log(output.join(","));
  }
);
