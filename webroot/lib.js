const sqlite3 = require('sqlite3').verbose();


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


async function genericInsertAndGet(db, table, cols, returnCol) {
    await genericInsert(db, table, [cols]);
    return await getOneCol(db, table, cols, returnCol);
}


async function getOneCol(db, table, cols, returnCol) {
    let res = await getAll(db, table, cols);
    if (res.length == 0) {
        throw new Error(`No records returned from ${table} / ${cols}`);
    }
    let r = res[0][returnCol];
    if (r === undefined) {
        throw new Error(`undefined from ${table} / ${cols} expecting return column ${returnCol}`);
    }
    return r;
}

function getAll(db, table, cols) {
    let wheres = [];
    let vals = [];
    Object.entries(cols).forEach(([k, v]) => {
        wheres.push(
            (v !== null) ?
            ` ${k.replace(/[^a-z0-9A-Z_]/g, '_')} = ? ` :
            ` ${k.replace(/[^a-z0-9A-Z_]/g, '_')} is ? `
        );
        vals.push(v);
    });
    let sql = `select * from ${table} where ${wheres.join('and')}`

    return dbAll(db, sql, vals);
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


let createTables = [
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
        '    "race_length" TEXT,',
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
        '    "race_driver_laps_position" INTEGER',
        ') STRICT'
    ].join("\n"),
];


async function createSchema(db) {

    let statements = [

        ...createTables,

        [
            'CREATE UNIQUE INDEX "uq_race_driver" ON',
            'race_driver (race_id, driver_id)'
        ].join("\n"),

        [
            'CREATE UNIQUE INDEX "uq_driver" ON',
            'driver (driver_name COLLATE NOCASE)'
        ].join("\n"),

        [
            'CREATE UNIQUE INDEX "uq_club" ON',
            'club (club_subdomain)'
        ].join("\n"),

        [
            'CREATE UNIQUE INDEX "uq_event" ON',
            'event (event_date, event_name COLLATE NOCASE, club_id)'
        ].join("\n"),

        [
            'CREATE UNIQUE INDEX "uq_race" ON',
            'race (heat_id, race_number)'
        ].join("\n"),

        [
            'CREATE UNIQUE INDEX "uq_race_class" ON',
            'race_class (race_class_name COLLATE NOCASE)'
        ].join("\n"),
    ];

    for (const stmt of statements) {
        await dbRun(db, stmt, []);
    }

    return true;

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


function getCurrentQuarter() {
    const currentDate = new Date();
    const quarter = Math.floor(currentDate.getMonth()/3) + 1;
    const year = currentDate.getFullYear();
    return { quarter, year };
}


async function genericInsert(db, tablename, records) {
    if (records.length == 0) {
        return;
    }
    let keys = Object.keys(records[0]);
    let sqlTableColumn = keys.map((k) => k.replace(/[^a-z0-9_]/g, ''));
    let sqlQuestionMarks = keys.map((k) => '?');
    let sql = `
        INSERT INTO ${tablename} (${sqlTableColumn.join(', ')})
            VALUES (${sqlQuestionMarks.join(', ')}) ON CONFLICT DO NOTHING`;
    const stmt = db.prepare(sql);
    for (const record of records) {
        let values = keys.map((k) => record[k]);
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


function isNodeJS() {
    if (typeof window === 'undefined') {
        return true;
    }
    return false;
}


module.exports = {
    getDatabase,
    dbRunStatement,
    dbFinalizeStatement,
    getCurrentQuarter,
    stringMillis,
    dbRun,
    getAll,
    genericInsert,
    dbAll,
    isNodeJS,
    getOneCol,
    genericInsertAndGet,
    createSchema,
    createTables
};


