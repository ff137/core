/**
 * Interface to Cassandra client
 * */
import { Client } from "cassandra-driver";
import { parse } from "url";
import { CASSANDRA_URL } from "../config";

const spl = CASSANDRA_URL.split(",");
const cps = spl.map((u) => parse(u).host);
console.log("connecting %s", CASSANDRA_URL);
const cassandra = new Client({
  contactPoints: cps,
  localDataCenter: "datacenter1",
  keyspace: parse(spl[0]).path.substring(1),
});
export default cassandra;
