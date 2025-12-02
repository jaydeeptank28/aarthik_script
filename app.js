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

function readExcel(filePath) {
    const workbook = XLSX.readFile(filePath);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    return XLSX.utils.sheet_to_json(sheet);
}

function buildAdditionalInfo(row, skip) {
    const arr = [];
    for (const key in row) {
        if (!row[key]) continue;
        if (skip.includes(key)) continue;
        arr.push(`${key} : ${row[key]}`);
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
                $${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}, $${base+7},
                $${base+8}, $${base+9}, $${base+10}, $${base+11}, $${base+12}, $${base+13},
                $${base+14}, $${base+15}, $${base+16}
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

async function createLog(fileName) {
    const result = await db.query(
        `INSERT INTO import_logs (file_name, start_time, status)
         VALUES ($1, NOW(), 'pending') RETURNING id`,
        [fileName]
    );
    return result.rows[0].id;
}

async function updateLog(logId, stats, status) {
    await db.query(
        `UPDATE import_logs
         SET end_time = NOW(),
             total_rows = $1,
             inserted_rows = $2,
             failed_rows = $3,
             status = $4,
             total_seconds = EXTRACT(EPOCH FROM (NOW() - start_time))
         WHERE id = $5`,
        [
            stats.total_rows,
            stats.inserted_rows,
            stats.failed_rows,
            status,
            logId
        ]
    );
}

async function importFile(filePath, logId) {
    const rows = readExcel(filePath);

    if (!rows.length) {
        await updateLog(logId, { total_rows: 0, inserted_rows: 0, failed_rows: 0 }, "failed");
        return { stop: true, error: "Empty file. No rows found." };
    }

    const firstRow = rows[0];
    const headers = Object.keys(firstRow).map(h => h.toLowerCase().trim());

    const required = ["name", "phone"];
    const missing = required.filter(col => !headers.includes(col));

    if (missing.length > 0) {
        await updateLog(logId, { total_rows: 0, inserted_rows: 0, failed_rows: 0 }, "failed");
        return { stop: true, error: `Missing required columns: ${missing.join(", ")}` };
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

        const isDuplicate = await findDuplicateByPhone(row.phone);
        if (isDuplicate) {
            failedRows.push({ row: index, reason: "Duplicate phone" });
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

    await updateLog(
        logId,
        {
            total_rows: totalCount,
            inserted_rows: insertedCount,
            failed_rows: failedRows.length
        },
        "complete"
    );

    return {
        total_rows: totalCount,
        inserted_rows: insertedCount,
        failed_rows: failedRows.length,
        failed_details: failedRows
    };
}

app.post("/api/import-file", upload.single("file"), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, error: "No file uploaded" });
        }

        const fileName = req.file.originalname;
        const filePath = req.file.path;

        const logId = await createLog(fileName);

        const result = await importFile(filePath, logId);

        fs.unlinkSync(filePath);

        if (result.stop) {
            return res.status(400).json({ success: false, error: result.error });
        }

        return res.json({
            success: true,
            message: "File imported successfully",
            stats: result,
            log_id: logId
        });

    } catch (err) {
        return res.status(500).json({ success: false, error: err.message });
    }
});

app.listen(4000, () => console.log("Server running on 4000"));
