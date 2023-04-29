import { createGunzip } from "zlib";
import JSONStream, { parse } from "JSONStream";
import { createReadStream } from "fs";

const fileName = "../export/dump.json.gz";
const write = createReadStream(fileName);
const stream = parse("*.match_id");

stream.on("data", (d) => {
  console.log(d);
});

write.pipe(createGunzip()).pipe(JSONStream);
