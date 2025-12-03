const XLSX = require("xlsx");
const fs = require("fs");
const mysql = require("mysql2/promise");

const db = mysql.createPool({
    host: "localhost",
    user: "root",
    password: "1234",
    database: "aarthik_script",
    waitForConnections: true,
    connectionLimit: 10
});

function readWorkbook(filePath) {
    return XLSX.readFile(filePath);
}

function toCamelCase(str) {
    return str
        .toLowerCase()
        .split(/[^a-zA-Z0-9]+/)
        .filter(Boolean)
        .map((word, index) => (index === 0 ? word : word[0].toUpperCase() + word.slice(1)))
        .join("");
}

function buildAdditionalInfo(row, skip) {
    const arr = [];
    for (const key in row) {
        if (!row[key]) continue;
        if (skip.includes(key)) continue;

        const camelKey = toCamelCase(key);
        arr.push(`${camelKey} : ${row[key]}`);
    }
    return arr.join(" | ");
}

async function findDuplicateByPhone(phone) {
    const [rows] = await db.query(
        `SELECT id FROM ats_uploaded_contacts WHERE phone = ? LIMIT 1`,
        [phone]
    );
    return rows.length > 0;
}

async function insertBatch(batch) {
    const conn = await db.getConnection();
    try {
        await conn.beginTransaction();

        const placeholders = batch
            .map(() => "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .join(",");

        const query = `
            INSERT INTO ats_uploaded_contacts
            (name, email, phone, area, city, state, zip, address,
             is_converted_to_prospect, is_converted_to_lead, prospect_status,
             status_id, assign_to, lead_type, company_name, position, additional_info)
            VALUES ${placeholders}
        `;

        const values = batch.flatMap(r => [
            r.name, r.email, r.phone, r.area, r.city, r.state, r.zip, r.address,
            0, 0, null,
            4, null, "sales", null, null,
            r.additional_info
        ]);

        await conn.query(query, values);
        await conn.commit();
    } catch (err) {
        await conn.rollback();
        throw err;
    } finally {
        conn.release();
    }
}

async function createLog(fileName, sheetName) {
    const [result] = await db.query(
        `INSERT INTO ats_upload_logs (file_name, sheet_name, start_time, status)
         VALUES (?, ?, NOW(), 'pending')`,
        [fileName, sheetName]
    );
    return result.insertId;
}

async function updateLog(logId, stats, status, totalMs) {
    await db.query(
        `UPDATE ats_upload_logs
         SET end_time = NOW(),
             total_rows = ?,
             inserted_rows = ?,
             failed_rows = ?,
             status = ?,
             total_seconds = ?
         WHERE id = ?`,
        [
            stats.total_rows,
            stats.inserted_rows,
            stats.failed_rows,
            status,
            Math.round(totalMs / 1000),
            logId
        ]
    );
}

async function importFile(filePath) {
    const fileName = filePath.split("/").pop();
    const workbook = readWorkbook(filePath);

    let sheetResults = [];

    for (const sheetName of workbook.SheetNames) {

        const sheetStart = Date.now();
        const sheet = workbook.Sheets[sheetName];
        const rows = XLSX.utils.sheet_to_json(sheet);

        const logId = await createLog(fileName, sheetName);

        if (!rows.length) {
            const sheetEnd = Date.now();
            await updateLog(
                logId,
                { total_rows: 0, inserted_rows: 0, failed_rows: 0 },
                "failed",
                sheetEnd - sheetStart
            );
            sheetResults.push({ sheet: sheetName, success: false, error: "Empty sheet", log_id: logId });
            continue;
        }

        const headers = Object.keys(rows[0]).map(h => h.toLowerCase().trim());
        const required = ["name", "phone"];
        const missing = required.filter(col => !headers.includes(col));

        if (missing.length > 0) {
            const sheetEnd = Date.now();
            await updateLog(
                logId,
                { total_rows: 0, inserted_rows: 0, failed_rows: 0 },
                "failed",
                sheetEnd - sheetStart
            );
            sheetResults.push({
                sheet: sheetName,
                success: false,
                error: `Missing required columns: ${missing.join(", ")}`,
                log_id: logId
            });
            continue;
        }

        const skip = ["name", "email", "phone", "area", "city", "state", "zip", "address"];
        let insertedCount = 0;
        let failedRows = [];
        const totalCount = rows.length;

        const batch = [];
        let index = 0;

        for (const r of rows) {
            index++;

            const row = {};
            for (const k in r) row[k.toLowerCase().trim()] = r[k];

            if (!row.name) {
                failedRows.push({ row: index, reason: "Missing name" });
                continue;
            }

            if (!row.phone) {
                failedRows.push({ row: index, reason: "Missing phone" });
                continue;
            }

            batch.push({
                name: row.name,
                email: row.email || null,
                phone: row.phone,
                area: row.area || null,
                city: row.city || null,
                state: row.state || null,
                zip: row.zip || null,
                address: row.address || null,
                additional_info: buildAdditionalInfo(row, skip)
            });

            if (batch.length === 1000) {
                await insertBatch(batch);
                insertedCount += batch.length;
                batch.length = 0;
            }
        }

        if (batch.length) {
            await insertBatch(batch);
            insertedCount += batch.length;
        }

        const sheetEnd = Date.now();

        await updateLog(
            logId,
            {
                total_rows: totalCount,
                inserted_rows: insertedCount,
                failed_rows: failedRows.length
            },
            "complete",
            sheetEnd - sheetStart
        );

        sheetResults.push({
            sheet: sheetName,
            success: true,
            stats: {
                total_rows: totalCount,
                inserted_rows: insertedCount,
                failed_rows: failedRows.length,
                failed_details: failedRows
            },
            log_id: logId
        });
    }

    return sheetResults;
}

async function run() {
    const files = process.argv.slice(2);

    if (files.length === 0) {
        console.log("Usage: node import-script.js file1.xlsx file2.xlsx ...");
        process.exit(1);
    }

    console.log(`Starting import for ${files.length} file(s)...\n`);

    for (const filePath of files) {
        console.log(`Processing: ${filePath}`);

        const result = await importFile(filePath);

        console.log("Done:", result);
        console.log("-------------------------------------------");
    }

    console.log("All files processed.");
    process.exit(0);
}

run();
