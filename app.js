const express = require("express");
const multer = require("multer");
const XLSX = require("xlsx");
const fs = require("fs");
const { Pool } = require("pg");

const app = express();
app.use(express.json());

const db = new Pool({
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "2628",
    database: "aarthik_script"
});

const upload = multer({ dest: "uploads/" });

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
    const result = await db.query(
        `SELECT id FROM ats_uploaded_contacts WHERE phone = $1 LIMIT 1`,
        [phone]
    );
    return result.rows.length > 0;
}

async function insertBatch(batch) {
    const client = await db.connect();
    try {
        await client.query("BEGIN");

        const query = `
        INSERT INTO ats_uploaded_contacts
        (name, email, phone, area, city, state, zip,
         is_converted_to_prospect, is_converted_to_lead, prospect_status,
         status_id, assign_to, lead_type, company_name, position, additional_info)
        VALUES 
        ${batch.map((_, i) => {
            const base = i * 16;
            return `(
                $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7},
                $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13},
                $${base + 14}, $${base + 15}, $${base + 16}
            )`;
        }).join(",")}
        `;

        const values = batch.flatMap(r => [
            r.name, r.email, r.phone, r.area, r.city, r.state, r.zip,
            false, false, null,
            4, null, "sales", null, null,
            r.additional_info
        ]);

        await client.query(query, values);
        await client.query("COMMIT");
    } catch (err) {
        await client.query("ROLLBACK");
        throw err;
    } finally {
        client.release();
    }
}

async function createLog(fileName, sheetName) {
    const result = await db.query(
        `INSERT INTO import_logs (file_name, sheet_name, start_time, status)
         VALUES ($1, $2, NOW(), 'pending') RETURNING id`,
        [fileName, sheetName]
    );
    return result.rows[0].id;
}

async function updateLog(logId, stats, status, totalMs) {
    await db.query(
        `UPDATE import_logs
         SET end_time = NOW(),
             total_rows = $1,
             inserted_rows = $2,
             failed_rows = $3,
             status = $4,
             total_seconds = $5
         WHERE id = $6`,
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

async function importFile(filePath, fileName) {
    const workbook = readWorkbook(filePath);
    let sheetResults = [];

    for (const sheetName of workbook.SheetNames) {

        const sheetStart = Date.now();

        const sheet = workbook.Sheets[sheetName];
        const rows = XLSX.utils.sheet_to_json(sheet);

        const logId = await createLog(fileName, sheetName);

        if (!rows.length) {
            const sheetEnd = Date.now();
            await updateLog(logId, { total_rows: 0, inserted_rows: 0, failed_rows: 0 }, "failed", sheetEnd - sheetStart);

            sheetResults.push({
                sheet: sheetName,
                success: false,
                error: "Empty sheet",
                log_id: logId
            });

            continue;
        }

        const headers = Object.keys(rows[0]).map(h => h.toLowerCase().trim());
        const required = ["name", "phone"];
        const missing = required.filter(col => !headers.includes(col));

        if (missing.length > 0) {
            const sheetEnd = Date.now();
            await updateLog(logId, { total_rows: 0, inserted_rows: 0, failed_rows: 0 }, "failed", sheetEnd - sheetStart);

            sheetResults.push({
                sheet: sheetName,
                success: false,
                error: `Missing required columns: ${missing.join(", ")}`,
                log_id: logId
            });

            continue;
        }

        const skip = ["name", "email", "phone", "area", "city", "state", "zip"];

        let insertedCount = 0;
        let failedRows = [];
        let totalCount = rows.length;

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

app.post("/api/import-files", upload.array("files", 50), async (req, res) => {
    try {
        if (!req.files || req.files.length === 0) {
            return res.status(400).json({ success: false, error: "No files uploaded" });
        }

        let results = [];

        for (const file of req.files) {
            const fileName = file.originalname;
            const filePath = file.path;

            const sheetResults = await importFile(filePath, fileName);

            fs.unlinkSync(filePath);

            results.push({
                file: fileName,
                sheets: sheetResults
            });
        }

        return res.json({
            success: true,
            message: "All files processed",
            files: results
        });

    } catch (err) {
        return res.status(500).json({
            success: false,
            error: err.message
        });
    }
});


app.listen(4000, () => console.log("Server running on 4000"));
