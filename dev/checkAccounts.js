/* eslint-disable */
import { readFileSync } from "fs";
import { SteamClient, SteamUser, EResult } from "steam";
import { whilst } from "async";

const accountData = readFileSync("./STEAM_ACCOUNT_DATA_BAD.txt", "utf8");
const accountArray = accountData.split(require("os").EOL);

let index = Number(process.argv[2]) || -1;
whilst(
  () => true,
  (cb) => {
    index += 1;
    const random = index;
    // const random = Math.floor(Math.random() * accountArray.length);
    const user = accountArray[random].split("\t")[0];
    const pass = accountArray[random].split("\t")[1];
    const logOnDetails = {
      account_name: user,
      password: pass,
    };
    const client = new SteamClient();
    client.steamUser = new SteamUser(client);
    client.connect();
    client.on("connected", () => {
      client.steamUser.logOn(logOnDetails);
    });
    client.on("logOnResponse", (logOnResp) => {
      if (logOnResp.eresult === EResult.AccountDisabled) {
        console.error(index, user, "failed", logOnResp.eresult);
      } else if (logOnResp.eresult === EResult.InvalidPassword) {
        console.error(index, user, "failed", logOnResp.eresult);
      } else {
        console.error(index, user, "passed", logOnResp.eresult);
      }
      client.disconnect();
      setTimeout(cb, 500);
    });
  },
  () => {}
);
