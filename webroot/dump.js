const {
    getDatabase,
    dbRunStatement,
    dbFinalizeStatement,
    getCurrentQuarter,
    stringMillis,
    dbRun,
    genericInsert,
    dbAll,
    getAll,
    isNodeJS,
    getOneCol,
    genericInsertAndGet,
    createSchema } = require('./lib.js');
const fs = require('fs/promises');
const md5 = require('md5');
const path = require('path');
const { walkSimple } = require('walk-directories');
const prqljs = require("prql-js"); 



function runAndDump(db, outfile, sql, prql, params, asKV) {
    return fs.mkdir(path.dirname(outfile), { recursive: true })
        .then(() => {
              if (prql) {
                  sql = prqljs.compile(prql);
                  console.log(">>>", sql);
              }
              return sql;
        })
        .then((sql) => dbAll(db, sql, params))
        .then((res) => {
            if (asKV) {
                let kv = {};
                for (r of res) {
                    let k = asKV.hasOwnProperty("func") ? asKV["func"](r[asKV["col"]]) : r[asKV["col"]];
                    if (kv.hasOwnProperty(k)) {
                        throw new Error(`Found existing key "${k}"`);
                    }
                    delete r[asKV["col"]];
                    kv[k] = r;
                }
                res = kv;
            }
            // console.log(">", res);
            console.table(res);
            return fs.writeFile(
                outfile,
                JSON.stringify(res)
            );
        });
}

let club = process.argv[process.argv.length - 1];
getDatabase(club)
    .then(async (db) => {
        return db;
    })
    .then(async (db) => {
        let toProc = [
            // { filename: `jsons/${club}/club.json`, sql: 'select * from club', params: [] },
            // { filename: `jsons/${club}/event.json`, sql: 'select * from event', params: [] },
            // { filename: `jsons/${club}/heat.json`, sql: 'select * from heat', params: [] },
            // { filename: `jsons/${club}/race.json`, sql: 'select * from race', params: [] },
            // { filename: `jsons/${club}/race_class.json`, sql: 'select * from race_class', params: [] },
            // { filename: `jsons/${club}/race_driver.json`, sql: 'select * from race_driver', params: [] },
            // { filename: `jsons/${club}/race_driver_laps.json`, sql: 'select * from race_driver_laps', params: [] },
            // { filename: `jsons/${club}/driver.json`, sql: 'select driver_name as k, driver_name from driver', params: [], k: { col: "k", func: md5 } },
             {
                 filename: `jsons/${club}/results.json`,
                 prql: `
prql target:sql.sqlite

let date_to_text = for dat -> s"strftime({for}, {dat})"
let min_week = $1
let max_week = $2

let event_weeks = (
    from event
    derive { event_week = event_date | (date_to_text "%Y-%W") }
    filter event_week >= min_week
    filter event_week <= max_week
    select { event_id, event_week, event_date }
)

let identify_finals = (
    from race
    join heat ( heat.heat_id==race.heat_id )
    join event_weeks ( event_weeks.event_id==heat.event_id )
    select { race_id, heat_id, event_id }
    # select { heat_id, event_id }
    # group event_id, heat_id (
    #     sort heat_id
    #     derive {
    #         heat_id = max heat_id,
    #         heat_seq = row_number this
    #     }
    # )
    # select { event_id, heat_id, heat_seq }
)

# let heat_number = (
#     from heat
#     select { heat_id, event_id }
#     group event_id (
#         derive { heat_id = row_number this}
#     )
#     join event_weeks ( ==event_id )
#     select { event_id, heat_id }
# )
# 
# let races = (
#     from race
#     join heat ( == heat_id )
#     join event_weeks ( == event_id )
#     group heat_id (
#         derive { race_num_in_heat = row_number this }
#     )
#     select {
#         heat.event_id,
#         race.heat_id,
#         race_num_in_heat,
#         race.race_id,
#         heat.heat_name,
#         race.race_class_id,
#         race.race_logged_class_name
#      }
#     group {event_id, race_class_id} (
#         derive {
#             is_final = heat_id == max heat_id,
#         }
#     )
#     group {event_id, heat_id} (
#         derive {
#             rr = row_number this,
#         }
#     )
# )

from identify_finals
# from races
# select { event_id, heat_id, race_num_in_heat, race_id, heat_name, race_logged_class_name }
# sort race_id
                     `,
                 params: ['2023-32', '2023-32'],
                 // params: [],
             },

        ];
        for ({filename, sql, prql, params, k} of toProc) {
            await runAndDump(db, filename, sql, prql, params, k);
        }
        dbAll(db, 'select * from club', [])
        return db;
    })
    .then((db) => {
        db.close();
    })
    .catch((err) => {
        console.error(err);
    })

