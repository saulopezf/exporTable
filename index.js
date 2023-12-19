const mysql = require('mysql2/promise');
const Client = require("pg").Client;
const fs = require("fs/promises");

const args = process.argv.slice(2);

const POSTGRES = "pg";
const MYSQL = "mysql"

const USER = "--user";
const PASS = "--pass";
const HOST = "--host";
const PORT = "--port";
const NAME = "--db";
const TYPE = "--type";

async function connect(type, host, port, name, user, pass) {
    switch(type) {
        case POSTGRES:
            const client = new Client({
                user,
                database: name,
                password: pass,
                port,
                host,
            });
            await client.connect();
            return client;
        case MYSQL:
            return mysql.createPool({
                host,
                port,
                database: name,
                user,
                password: pass,
                waitForConnections: true,
                connectionLimit: 10,
                maxIdle: 10,
                idleTimeout: 60000,
                queueLimit: 0,
                enableKeepAlive: true,
                keepAliveInitialDelay: 0
            });
        default:
            throw new Error("You must insert database type");
    }
}

async function exportTable(connection, dbType, table) {
    switch(dbType) {
        case POSTGRES:
            const pgResult = await connection.query(`SELECT * FROM ${table}`);
            return pgResult.rows;
        case MYSQL:
            return await connection.query(`SELECT * FROM ${table}`);
    }
}

async function main(args) {
    try {
        if(args.length === 0) 
            throw new Error("cli: <table> --host 127.0.0.1 --db dbname --user user --pass pass --port 5440 --type mysql");
        if(args[0].includes("--"))
            throw new Error("First arg must be a table");
        // console.log(args);

        const table = args[0];
        const maxInsert = 10000;
        let user, pass, host, name, port, type;

        for(let i = 0; i < args.length; i++) {
            switch(args[i]) {
                case USER:
                    user = args[i+1];
                    i++
                    break;
                case PASS:
                    pass = args[i+1];
                    i++
                    break;
                case HOST:
                    host = args[i+1];
                    i++
                    break;
                case PORT:
                    port = args[i+1];
                    i++
                    break;
                case NAME:
                    name = args[i+1];
                    i++
                    break;
                case TYPE:
                    type = args[i+1];
                    i++
                    break;
            }
        }
        console.log(`Conecting...\nhost: ${host}\nport: ${port}\ndb--: ${name}\nuser: ${user}\npass: ${pass}\n`);
        const db = await connect(type, host, port, name, user, pass);
        const tableExported = await exportTable(db, type, table);
        const rows = tableExported.length;
        if(!Array.isArray(tableExported))
            throw new Error("Something went wrong exporting the table.");

        if(rows === 0) {
            console.log("Nothing to export in this table");
            process.exit(0);
        }
        
        let sql = "";
        console.log(`Exporting ${rows}...\n`);
        
        for(let bulk = 0; bulk < rows; bulk += maxInsert){
            const subArray = tableExported.slice(bulk, bulk + maxInsert);
            sql += `INSERT INTO "${table}" (${Object.keys(subArray[0]).map(column => `"${column}"`).join(",")}) VALUES\n`
            for(let i = 0; i < subArray.length; i++) {
                const values = Object.values(subArray[i])
                sql += "(";
                for(let j = 0; j < values.length; j++) {
                    sql += values[j] != 0 && !values[j] ? "NULL" : typeof values[j] === "string" ? `'${values[j].replace(/[']/g, "'$&")}'` : values[j];
                    if(j + 1 < values.length) sql += ","
                }
                sql += ")";
                sql += (i + 1) === subArray.length ? "" : ",\n";
            }
            sql += ";\n"
        }

        console.log("Writing sql file...\n");
        await fs.writeFile(`${__dirname}/results/${Date.now()}-${table}.sql`, sql);
        
        process.exit(0);
    } catch (error) {
        console.log(error);
        process.exit(1);
    }
}

process.on('exit', (code) => {
    console.log(`Exit with code: ${code}`);
});

main(args);