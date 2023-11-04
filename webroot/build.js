const sqlite3 = require('sqlite3').verbose();
const fs = require('fs/promises');


function getDatabase(club) {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database('db/' + club + '.sqlite', (err) => {
            if (err) { return reject(err); }
            resolve(db);
        });
    });
}


function dbRunStatement(stmt, values) {
    return new Promise((resolve, reject) => {
        stmt.run(values, function (err, res) {
            if (err) {
                return reject(err);
            }
            resolve(this);
        });
    });
}


function dbFinalizeStatement(stmt, values) {
    return new Promise((resolve, reject) => {
        stmt.finalize(function (err, res) {
            if (err) { return reject(err); }
            resolve(res);
        });
    });
}


function dbRun(db, stmt, values) {
    return new Promise((resolve, reject) => {
        db.run(stmt, values, function (err, res) {
            if (err) {
                return reject(err);
            }
            resolve(this);
        });
    });
}


async function createSchema(db) {

    let statements = [
        [
            'CREATE TABLE "driver" (',
            '    "driver_id" INTEGER PRIMARY KEY,',
            '    "driver_name" TEXT NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "club" (',
            '    "club_id" INTEGER PRIMARY KEY,',
            '    "club_name" TEXT NOT NULL,',
            '    "club_subdomain" TEXT UNIQUE NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "event" (',
            '    "event_id" INTEGER PRIMARY KEY,',
            '    "event_name" TEXT NOT NULL,',
            '    "event_date" TEXT NOT NULL,',
            '    "club_id" INTEGER NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "heat" (',
            '    "heat_id" INTEGER PRIMARY KEY,',
            '    "event_id" INTEGER,',
            '    "heat_name" TEXT NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "race_class" (',
            '    "race_class_id" INTEGER PRIMARY KEY,',
            '    "race_class_name" TEXT UNIQUE NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "race" (',
            '    "race_id" INTEGER PRIMARY KEY,',
            '    "race_number" INTEGER NOT NULL,',
            '    "race_logged_class_name" TEXT NOT NULL,',
            '    "race_length" TEXT NOT NULL,',
            '    "race_round" TEXT NOT NULL,',
            '    "race_class_id" INTEGER NOT NULL,',
            '    "heat_id" INTEGER NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "race_driver" (',
            '    "race_driver_id" INTEGER PRIMARY KEY,',
            '    "race_id" INTEGER NOT NULL,',
            '    "driver_id" INTEGER NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE TABLE "race_driver_laps" (',
            '    "race_driver_id" INTEGER NOT NULL,',
            '    "race_driver_laps_lap_number" INTEGER NOT NULL,',
            '    "race_driver_laps_time_millisecond" INTEGER NOT NULL,',
            '    "race_driver_laps_position" INTEGER NOT NULL',
            ') STRICT'
        ].join("\n"),

        [
            'CREATE UNIQUE INDEX "uq_race_driver" ON',
            'race_driver (race_id, driver_id)'
        ].join("\n"),
    ];

    for (const stmt of statements) {
        await dbRun(db, stmt, []);
    }

    return true;

}


function getRacerLaps(race) {
    return (race?.driver_results?.racerLaps || []);
}

function getEventJSONRecord(clubId, race) {
    let date = new Date(Date.parse(race.date));
    if (date.getUTCHours() > 12) {
        date = new Date(date.getTime() + (1000 * 60 * 60 * 12));
    }
    return [{
        club_id: clubId,
        event_id: parseInt(race.event_id),
        event_name: race.event,
        event_date: date.toISOString().replace(/T.*/, '')
    }];
}


function stringMillis(str) {
    while (true) {
        if (str.indexOf('.') == -1) {
            str = str + '.';
        }
        if (str.replace(/.*\./, '').length >= 3) {
            let res = str.match(/(.*)\.([0-9][0-9][0-9])/);
            return parseInt("" + res[1] + res[2]);
        }
        str = str + '0';
    }
    return parseInt(str);
}


function getRaceDriverLapsJSONRecord(race) {

    function lapsMapper(joinData, lap) {
        return {
            ...joinData,
            race_driver_laps_lap_number: parseInt(lap.lapNum),
            race_driver_laps_time_millisecond: stringMillis(lap.time),
            race_driver_laps_position: parseInt(lap.pos)
        };
    };

    let racerLaps = getRacerLaps(race);
    return Object.entries(racerLaps).reduce((acc, [driverId, details]) => {
        let joinData = {
            driver_id: parseInt(driverId),
            race_id: parseInt(race.race_id),
        };
        let toAdd = details.laps.map(lapsMapper.bind(null, joinData));
        return [...acc, toAdd];
    }, []).flat();
}


function getHeatJSONRecord(race) {
    return [{
        heat_id: parseInt(race.heat_id),
        event_id: parseInt(race.event_id),
        heat_name: race.round,
    }];
}


function getRaceClass(raceClassStr) {
    return raceClassStr.replace(/ +\(.*/, '').replace(/ +[A-Z0-9]+\-Main$/, '')
}


function getRaceClassJSONRecord(race) {
    return [{
        race_class_name: getRaceClass(race.raceClass)
    }];
}

function getRaceJSONRecord(raceClassIds, race) {
    return [{
        race_id: race.race_id,
        race_number: race.raceNum,
        race_logged_class_name: race.raceClass,
        race_length: race.length,
        race_round: race.round,
        heat_id: race.heat_id,
        race_class_id: raceClassIds[getRaceClass(race.raceClass)]
    }];
}


function getRaceDriverJSONRecord(race) {
    return Object.keys(getRacerLaps(race)).map((driverId) => {
        return { driver_id: parseInt(driverId), race_id: race.race_id };
    });
}


function getDriverJSONRecord(race) {
    let racerLaps = getRacerLaps(race);
    return Object.entries(racerLaps).map(([k, details]) => {
        return { driver_id: parseInt(k), driver_name: details.driverName };
    });
}


function getCurrentQuarter() {
    const currentDate = new Date();
    const quarter = Math.floor(currentDate.getMonth()/3) + 1;
    const year = currentDate.getFullYear();
    return { quarter, year };
}


function decrQuarter(quarter /* { year, quarter } */) {
    if (quarter.quarter == 1) {
        return { year: quarter.year - 1, quarter: 4 };
    }
    return { year: quarter.year, quarter: quarter.quarter - 1 };
}


function getJSONLocal(filename) {
    return fs.readFile("../output/" + filename, { encoding: 'utf8' })
        .then(content => JSON.parse(content));
}


function buildFilename(clubName, { quarter, year }, currentMarker) {
    return clubName + "/" + year + "-" + quarter + currentMarker + ".json";
}


async function genericInsert(db, tablename, records) {
    if (records.length == 0) {
        return;
    }
    let keys = Object.keys(records[0]);
    let sqlTableColumn = keys.map((k) => k.replace(/[^a-z0-9_]/g, ''));
    let sqlQuestionMarks = keys.map((k) => '?');
    let sql = `
        INSERT OR IGNORE INTO ${tablename} (${sqlTableColumn.join(', ')})
            VALUES (${sqlQuestionMarks.join(', ')})`;
    const stmt = db.prepare(sql);
    for (const record of records) {
        let values = keys.map((k) => record[k]);
        await dbRunStatement(stmt, values);
    }
    dbFinalizeStatement(stmt);
    return db;
}


async function insertRaceDriverLaps(db, records) {
    let sql = `
        with rd as (
          select race_driver_id, ?, ?, ? from race_driver
          where driver_id = ? and race_id = ?
        )
        insert into race_driver_laps (race_driver_id, race_driver_laps_lap_number, race_driver_laps_time_millisecond, race_driver_laps_position) select * from rd`;
    const stmt = db.prepare(sql);
    for (const record of records) {
        let values = [
            record.race_driver_laps_lap_number,
            record.race_driver_laps_time_millisecond,
            record.race_driver_laps_position,
            record.driver_id,
            record.race_id
        ];
        await dbRunStatement(stmt, values);
    }
    dbFinalizeStatement(stmt);
    return db;
}


function dbAll(db, select, values) {
    return new Promise((resolve, reject) => {
        db.all(select, values, function(err, res) {
            if (err) { return reject(err); }
            resolve(res);
        });
    });
}


function getClubId(db, clubDomain) {
    return dbAll(db, 'select * from club where club_subdomain = ?', [clubDomain])
        .then(recs => recs.reduce((acc, record) => {
            return record.club_id;
        }, -1));
}


function getRaceClassIds(db) {
    return dbAll(db, 'select * from race_class', [])
        .then(recs => recs.reduce((acc, record) => {
            return {...acc, [record["race_class_name"]]: record["race_class_id"] };
        }, {}));
}


async function processQuarter(db, clubId, racesInQuarter) {
    await genericInsert(db, 'driver', racesInQuarter.map(getDriverJSONRecord).flat());
    await genericInsert(
        db,
        'event',
        racesInQuarter.map(getEventJSONRecord.bind(null, clubId)).flat()
    );
    await genericInsert(db, 'heat', racesInQuarter.map(getHeatJSONRecord).flat());
    await genericInsert(db, 'race_class', racesInQuarter.map(getRaceClassJSONRecord).flat());
    let raceClassIds = await getRaceClassIds(db);
    await genericInsert(
        db,
        'race',
        racesInQuarter.map(getRaceJSONRecord.bind(null, raceClassIds)).flat()
    );
    await genericInsert(db, 'race_driver', racesInQuarter.map(getRaceDriverJSONRecord).flat());
    await insertRaceDriverLaps(db, racesInQuarter.map(getRaceDriverLapsJSONRecord).flat());
    return db;
}


async function importIntoDb(db, getJSON, clubDomain) {
    await createSchema(db);
    await dbRun(db, 'INSERT INTO club (club_subdomain, club_name) VALUES (?, ?)', [clubDomain, clubDomain]);
    let clubId = await getClubId(db, clubDomain);
    let quarter = getCurrentQuarter();
    let currentMarker = 'x';
    while (true) {
        let racesInQuarter
        console.log(quarter);
        let filename = buildFilename(clubDomain, quarter, currentMarker);
        quarter = decrQuarter(quarter);
        currentMarker = '';
        try {
            racesInQuarter = await getJSON(filename);
        } catch (e) {
            if (currentMarker != 'x') { break; }
        }
        await processQuarter(db, clubId, racesInQuarter);
    }
    return db;
}

function isNodeJS() {
    if (typeof window === 'undefined') {
        return true;
    }
    return false;
}

let club = process.argv[process.argv.length - 1];
getDatabase(club)
    .then((db) => {
        return importIntoDb(db, isNodeJS ? getJSONLocal : null, club);
    })
    .then(async (db) => {
        let basic = `
            select
                race.race_number,
                race.race_round,
                race_class.race_class_name,
                event.event_name,
                heat.heat_name,
                club.club_name,
                driver_name
            from race_driver
            inner join driver on driver.driver_id = race_driver.driver_id
            inner join race on race.race_id = race_driver.race_id
            inner join race_class on race_class.race_class_id = race.race_class_id
            inner join heat on heat.heat_id = race.heat_id
            inner join event on event.event_id = heat.event_id
            inner join club on club.club_id = event.club_id
            `;
        console.log(await dbAll(db, 'select * from race_driver', []));
        return db;
    })
    .then((db) => {
        db.close();
    })
    .catch((err) => {
        console.error(err);
    })

