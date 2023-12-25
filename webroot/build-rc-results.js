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
const path = require('path');
const { walkSimple } = require('walk-directories');


// function getRacerLaps(race) {
//     return (race?.driver_results?.racerLaps || []);
// }
// 
// function getEventJSONRecord(clubId, race) {
//     let date = new Date(Date.parse(race.date));
//     if (date.getUTCHours() > 12) {
//         date = new Date(date.getTime() + (1000 * 60 * 60 * 12));
//     }
//     return [{
//         club_id: clubId,
//         event_id: parseInt(race.event_id),
//         event_name: race.event,
//         event_date: date.toISOString().replace(/T.*/, '')
//     }];
// }
// 
// 
// function getRaceDriverLapsJSONRecord(race) {
// 
//     function lapsMapper(joinData, lap) {
//         return {
//             ...joinData,
//             race_driver_laps_lap_number: parseInt(lap.lapNum),
//             race_driver_laps_time_millisecond: stringMillis(lap.time),
//             race_driver_laps_position: parseInt(lap.pos)
//         };
//     };
// 
//     let racerLaps = getRacerLaps(race);
//     return Object.entries(racerLaps).reduce((acc, [driverId, details]) => {
//         let joinData = {
//             driver_id: parseInt(driverId),
//             race_id: parseInt(race.race_id),
//         };
//         let toAdd = details.laps.map(lapsMapper.bind(null, joinData));
//         return [...acc, toAdd];
//     }, []).flat();
// }
// 
// 
// function getHeatJSONRecord(race) {
//     return [{
//         heat_id: parseInt(race.heat_id),
//         event_id: parseInt(race.event_id),
//         heat_name: race.round,
//     }];
// }
// 
// 
// function getRaceClass(raceClassStr) {
//     return raceClassStr.replace(/ +\(.*/, '').replace(/ +[A-Z0-9]+\-Main$/, '')
// }
// 
// 
// function getRaceClassJSONRecord(race) {
//     return [{
//         race_class_name: getRaceClass(race.raceClass)
//     }];
// }
// 
// function getRaceJSONRecord(raceClassIds, race) {
//     return [{
//         race_id: race.race_id,
//         race_number: race.raceNum,
//         race_logged_class_name: race.raceClass,
//         race_length: race.length,
//         race_round: race.round,
//         heat_id: race.heat_id,
//         race_class_id: raceClassIds[getRaceClass(race.raceClass)]
//     }];
// }
// 
// 
// function getRaceDriverJSONRecord(race) {
//     return Object.keys(getRacerLaps(race)).map((driverId) => {
//         return { driver_id: parseInt(driverId), race_id: race.race_id };
//     });
// }
// 
// 
// function getDriverJSONRecord(race) {
//     let racerLaps = getRacerLaps(race);
//     return Object.entries(racerLaps).map(([k, details]) => {
//         return { driver_id: parseInt(k), driver_name: details.driverName };
//     });
// }
// 
// 
// function decrQuarter(quarter /* { year, quarter } */) {
//     if (quarter.quarter == 1) {
//         return { year: quarter.year - 1, quarter: 4 };
//     }
//     return { year: quarter.year, quarter: quarter.quarter - 1 };
// }
// 
// 
// function buildFilename(clubName, { quarter, year }, currentMarker) {
//     return clubName + "/" + year + "-" + quarter + currentMarker + ".json";
// }
// 
// 
// async function insertRaceDriverLaps(db, records) {
//     let sql = `
//         with rd as (
//           select race_driver_id, ?, ?, ? from race_driver
//           where driver_id = ? and race_id = ?
//         )
//         insert into race_driver_laps (race_driver_id, race_driver_laps_lap_number, race_driver_laps_time_millisecond, race_driver_laps_position) select * from rd`;
//     const stmt = db.prepare(sql);
//     for (const record of records) {
//         let values = [
//             record.race_driver_laps_lap_number,
//             record.race_driver_laps_time_millisecond,
//             record.race_driver_laps_position,
//             record.driver_id,
//             record.race_id
//         ];
//         await dbRunStatement(stmt, values);
//     }
//     dbFinalizeStatement(stmt);
//     return db;
// }


function getClubId(db, clubDomain) {
    return dbAll(db, 'select * from club where club_subdomain = ?', [clubDomain])
        .then(recs => recs.reduce((acc, record) => {
            return record.club_id;
        }, -1));
}


// function getRaceClassIds(db) {
//     return dbAll(db, 'select * from race_class', [])
//         .then(recs => recs.reduce((acc, record) => {
//             return {...acc, [record["race_class_name"]]: record["race_class_id"] };
//         }, {}));
// }
// 
// 
// async function processQuarter(db, clubId, racesInQuarter) {
//     await genericInsert(db, 'driver', racesInQuarter.map(getDriverJSONRecord).flat());
//     await genericInsert(
//         db,
//         'event',
//         racesInQuarter.map(getEventJSONRecord.bind(null, clubId)).flat()
//     );
//     await genericInsert(db, 'heat', racesInQuarter.map(getHeatJSONRecord).flat());
//     await genericInsert(db, 'race_class', racesInQuarter.map(getRaceClassJSONRecord).flat());
//     let raceClassIds = await getRaceClassIds(db);
//     await genericInsert(
//         db,
//         'race',
//         racesInQuarter.map(getRaceJSONRecord.bind(null, raceClassIds)).flat()
//     );
//     await genericInsert(db, 'race_driver', racesInQuarter.map(getRaceDriverJSONRecord).flat());
//     await insertRaceDriverLaps(db, racesInQuarter.map(getRaceDriverLapsJSONRecord).flat());
//     return db;
// }

async function processFile(db, filename, clubId, clubName) {
    let contents = JSON.parse(await fs.readFile(filename, { encoding: 'utf8' }));
    let startPos = JSON.parse(await fs.readFile(path.dirname(filename) + '/overview.json', { encoding: 'utf8' }))
        .reduce((acc, line) => {
            return line.Driver == contents.driver_meta.Driver ? parseInt(line.Car) : acc
        }, null);
    let contentsDead = {
      "laps": {
        "1": "14.88",
        "2": "12.34",
        "3": "12.66",
        "4": "13.43",
        "5": "12.86",
        "6": "12.79",
        "7": "12.41",
        "8": "16.62",
        "9": "12.51",
        "10": "12.79",
        "11": "12.55",
        "12": "12.50",
        "13": "13.50",
        "14": "12.90",
        "15": "18.52",
        "16": "12.76",
        "17": "12.25",
        "18": "12.49",
        "19": "12.60",
        "20": "12.77",
        "21": "11.95",
        "22": "12.26",
        "23": "12.45"
      },
      "driver_meta": {
        "Driver": "Neil Ralph",
        "Position": "1",
        "Result": "23 / 302.79",
        "Average": "13.16",
        "Best5": "12.24",
        "Best": "11.95",
        "Consec3": "36.66"
      },
      "race_meta": {
        "venue": "Southlakes MCC",
        "meeting": "19/11/2023 - Round 4",
        "isodate": "2023-11-19",
        "heat_name": "Finals - Round 2",
        "race_number": "Race 5",
        "race_class": "Vintage 2wd",
        "race_name": "Vintage 2wd - A Final",
        "rc_year_quarter": "2023-4"
      }
    };
    let driver_id = await genericInsertAndGet(
        db,
        'driver',
        { driver_name: contents.driver_meta.Driver },
        'driver_id'
    );
    let event_id = await genericInsertAndGet(
        db,
        'event',
        {
            event_date: contents.race_meta.isodate,
            event_name: contents.race_meta.meeting,
            club_id: clubId
        },
        'event_id'
    );
    let heat_id = await genericInsertAndGet(
        db,
        'heat',
        { event_id, heat_name: contents.race_meta.heat_name },
        'heat_id'
    );
    let race_class_id = await genericInsertAndGet(db, 'race_class', { race_class_name: contents.race_meta.race_class }, 'race_class_id');
    let race_id = await genericInsertAndGet(
        db,
        'race',
        {
            race_number: parseInt(contents.race_meta.race_number.replace(/.* /, '')),
            race_length: null,
            race_logged_class_name: contents.race_meta.race_name,
            race_round: contents.race_meta.heat_name,
            race_class_id,
            heat_id,
        },
        'race_id'
    );
    let race_driver_id = await genericInsertAndGet(
        db,
        'race_driver',
        { race_id, driver_id },
        'race_driver_id'
    );

    let laps = (Object.entries(contents.laps))
        .filter(([k]) => k !== 'Lap')
        .reduce(
            (acc, [k, v]) => {
                return [
                    ...acc,
                    {
                        race_driver_id,
                        race_driver_laps_lap_number: parseInt(k),
                        race_driver_laps_time_millisecond: stringMillis(v),
                        race_driver_laps_position: null,
                    }
                ];
            },
            [{
                race_driver_id,
                race_driver_laps_lap_number: 0,
                race_driver_laps_time_millisecond: 0,
                race_driver_laps_position: startPos,
            }]
        );
    laps[laps.length - 1].race_driver_laps_position = parseInt(contents.driver_meta.Position);
    // console.log(laps);
    // process.exit(1);

    await genericInsert(
        db,
        'race_driver_laps',
        laps
    );

    let stmt = db.prepare(
        `
            with
                race_driver_ids as (
                    select race_driver_id from race_driver where race_id = ?
                ),
                time_so_fars as (
                    select
                        race_driver_id,
                        race_driver_laps_lap_number,
                        sum(race_driver_laps_time_millisecond) over (
                            partition by race_driver_id
                            order by race_driver_laps_lap_number
                        ) as time_so_far
                    from race_driver_laps
                    where race_driver_id in (select * from race_driver_ids)
                ),
                calculated as (
                    select
                        race_driver_id,
                        race_driver_laps_lap_number,
                        time_so_far,
                        row_number() over (
                            partition by race_driver_laps_lap_number
                            order by time_so_far
                        ) as found_race_driver_laps_position
                    from time_so_fars
                ),
                last_lap as (
                    select race_driver_id,
                    max(race_driver_laps_lap_number) as race_driver_laps_lap_number
                    from calculated
                )
                update race_driver_laps
                set race_driver_laps_position = (
                    select found_race_driver_laps_position
                    from calculated
                    where race_driver_laps.race_driver_laps_lap_number = calculated.race_driver_laps_lap_number
                    and race_driver_laps.race_driver_id = calculated.race_driver_id
                )
                where race_driver_laps_lap_number > 0
                and (race_driver_id, race_driver_laps_lap_number) not in (
                    select race_driver_id, race_driver_laps_lap_number
                    from last_lap
                )
        `);
    await dbRunStatement(stmt, [race_id]);
    await dbFinalizeStatement(stmt);

    return db;
}


async function importIntoDb(db, getJSON, clubDomain) {
    await createSchema(db);
    let clubName = (await fs.readFile(`./rc-results/${clubDomain}/NAME`, { encoding: 'utf8' })).trim();
    await dbRun(db, 'INSERT INTO club (club_subdomain, club_name) VALUES (?, ?) ON CONFLICT DO NOTHING', [clubDomain, clubName]);
    let clubId = await getClubId(db, clubDomain);
    let files = (await walkSimple(`./rc-results/${clubDomain}/9937`, 'json'))
        .map(file => file.fullPath)
        .sort();
    for (file of files) {
        console.log(file);
        if (!file.match(/overview/)) {
            await processFile(db, file, clubId, clubName);
        }
    }
    // let quarter = getCurrentQuarter();
    // let currentMarker = 'x';
    // while (true) {
    //     let racesInQuarter
    //     console.log(quarter);
    //     let filename = buildFilename(clubDomain, quarter, currentMarker);
    //     quarter = decrQuarter(quarter);
    //     currentMarker = '';
    //     try {
    //         racesInQuarter = await getJSON(filename);
    //     } catch (e) {
    //         if (currentMarker != 'x') { break; }
    //     }
    //     await processQuarter(db, clubId, racesInQuarter);
    // }
    return db;
}


let club = process.argv[process.argv.length - 1];
getDatabase(club)
    .then((db) => {
        return importIntoDb(db, null, club);
    })
    .then(async (db) => {
        let basics = [
            `select * from driver`,
            `select * from event`,
            `select * from heat`,
            `select * from race_class`,
            `select * from race`,
            `select * from race_driver`,
            `
                with
                    race_driver_ids as (
                        select race_driver_id from race_driver where race_id = 1
                    )
                    select
                        race_driver_id,
                        race_driver_laps_lap_number,
                        sum(race_driver_laps_time_millisecond) over (
                            partition by race_driver_id
                            order by race_driver_laps_lap_number
                        ) as time_so_far,
                        race_driver_laps_position
                    from race_driver_laps
                    where race_driver_id in (select * from race_driver_ids)
                    order by race_driver_laps_lap_number, time_so_far
            `

        ];
        for (b of basics) {
            console.log(await dbAll(db, b, []));
        }
        return db;
    })
    .then((db) => {
        db.close();
    })
    .catch((err) => {
        console.error(err);
    })
