import { SteamClient, SteamUser, EResult } from "steam";
import { Dota2Client } from "dota2";

const client = new SteamClient();
const user = process.env.STEAM_USER;
const pass = process.env.STEAM_PASS;
const logOnDetails = {
  account_name: user,
  password: pass,
};
client.Dota2 = new Dota2Client(client, false, false);
client.Dota2.on("ready", () => {
  console.log("dota ready");
});
client.steamUser = new SteamUser(client);
client.connect();
client.on("connected", () => {
  console.log("[STEAM] Trying to log on with %s,%s", user, pass);
  client.steamUser.logOn(logOnDetails);
});
client.on("logOnResponse", (logOnResp) => {
  if (logOnResp.eresult !== EResult.OK) {
    // try logging on again
    console.error(logOnResp);
    client.steamUser.logOn(logOnDetails);
    return;
  }
  if (client && client.steamID) {
    console.log("[STEAM] Logged on %s", client.steamID);
    client.Dota2.launch();
  }
});
