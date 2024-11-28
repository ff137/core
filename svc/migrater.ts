import cassandra, { getCassandraColumns } from '../store/cassandra';
import redis from '../store/redis';
import scyllaDriver from 'cassandra-driver';

const scylla = new scyllaDriver.Client({
    contactPoints: ['odota-scylla'],
    localDataCenter: 'datacenter1',
    keyspace: 'yasp',
});

async function start() {
    const begin = BigInt(await redis.get('scyllaMigrateCheckpoint') ?? '-9223372036854775808');
    // Try out using COPY TO/COPY FROM (requires cqlsh)
    // Figure out approx how many rows are in data set and partition the token range so each export is reasonable
    // const result = await cassandra.execute(
    //     `COPY player_caches TO STDOUT WITH BEGINTOKEN = ?`,
    //     [begin.toString()],
    //     {
    //       prepare: true,
    //     },
    //   );
    // console.log(result);
    const allFields = await getCassandraColumns('player_caches');
    const result = await cassandra.execute(
        `select token(account_id) as tkn, ${Object.keys(allFields).join(', ')} from player_caches where token(account_id) >= ?`,
        [begin.toString()],
        {
            prepare: true,
            fetchSize: 1,
        },
    );
    result.rows.forEach(row => {
        // console.log(row.tkn.toString());
        // console.log(row);
        const obj: any = {};
        Object.keys(allFields).forEach(k => {
            if (row[k] !== null) {
                obj[k] = row[k].toString();
            }
        });
        console.log(obj, row.tkn.toString());
    });
    const nextToken = BigInt(result.rows[result.rows.length - 1].tkn.toString()) + BigInt(1);
    console.log(nextToken);
    // Copy from Cassandra to Scylla
    /*
    const query = util.format(
      'INSERT INTO player_caches (%s) VALUES (%s)',
      Object.keys(serializedMatch).join(','),
      Object.keys(serializedMatch)
        .map(() => '?')
        .join(','),
    );
    const arr = Object.keys(serializedMatch).map((k) => serializedMatch[k]);
    await scylla.execute(query, arr, {
      prepare: true,
    });
    */
    // Page through all rows using token range
    // Checkpoint progress to redis
    // Is output ordered by token?
    // Next value should be last row token + 1
    // When we get to the end we should find no more rows and stop
}
start();