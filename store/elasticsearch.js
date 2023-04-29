/**
 * Interface to ElasticSearch client
 * */
import { Client } from "@elastic/elasticsearch";
import { ELASTICSEARCH_URL, NODE_ENV } from "../config";

console.log("connecting %s", ELASTICSEARCH_URL);
const es = new Client({
  node: `http://${ELASTICSEARCH_URL}`,
  apiVersion: "6.8",
});

const INDEX = NODE_ENV === "test" ? "dota-test" : "dota";

export default {
  es,
  INDEX,
};
