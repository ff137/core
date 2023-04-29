import { parse } from "JSONStream";
import { select } from "../store/db";
import { mergeObjects } from "../util/utility";
import { count_words } from "../util/compute";

const args = process.argv.slice(2);
const limit = Number(args[0]) || 1;
const stream = select("chat")
  .from("matches")
  .where("version", ">", "0")
  .limit(limit)
  .orderBy("match_id", "desc")
  .stream();
const counts = {};
stream.on("end", () => {
  console.log(JSON.stringify(counts));
  process.exit(0);
});
stream.pipe(parse());
stream.on("data", (match) => {
  mergeObjects(counts, count_words(match));
});
