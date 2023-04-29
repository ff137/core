/* eslint-disable */
import request from "request";
import { loadProtoFile } from "protobufjs";
import { readdirSync, readFileSync } from "fs";
/*
const files = readdirSync('./proto');
const builder = ProtoBuf.newBuilder();
files.forEach((file) => {
  // console.log(file);
  loadProtoFile(`./proto/${file}`, builder);
});
*/
const builder = loadProtoFile("./proto/dota_match_metadata.proto");
const Message = builder.build();
const buf = readFileSync("./2750586075_1028519576.meta");
const message = Message.CDOTAMatchMetadataFile.decode(buf);
message.metadata.teams.forEach((team) => {
  team.players.forEach((player) => {
    player.equipped_econ_items.forEach((item) => {
      delete item.attribute;
    });
  });
});
delete message.private_metadata;
console.log(JSON.stringify(message, null, 2));
/*
const entries = [];
for (let i = 0; i < 1000000; i += 1) {
  entries.push({
    a: i,
    b: i / 7,
    c: 'asdf',
  });
}
console.time('JSON');
JSON.parse(JSON.stringify(entries));
console.timeEnd('JSON');
console.time('map');s
entries.map(e => Object.assign({}, e));
console.timeEnd('map');
*/
/*
import request from 'request';
import { eachSeries } from 'async';
eachSeries(Array.from(new Array(100), (e, i) => i), (i, cb) => {
  request(`http://localhost:5100?match_id=2716007205`, (err, resp, body) => {
    console.log(i, err, resp && resp.statusCode);
    setTimeout(() => {
      cb(err);
    }, 1000);
  });
}, (err) => (process.exit(Number(err))));
*/
