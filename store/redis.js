/**
 * Interface to Redis client
 * */
import { createClient } from "redis";
import { REDIS_URL } from "../config";

console.log("connecting %s", REDIS_URL);
const client = createClient(REDIS_URL, {
  detect_buffers: true,
});
client.on("error", (err) => {
  console.error(err);
  process.exit(1);
});
export default client;
