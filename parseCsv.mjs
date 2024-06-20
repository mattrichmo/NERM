import { readFileSync, writeFileSync } from 'fs';
import { parse } from 'csv-parse/sync';

const createUUID = () => {
    // creates a 9-digit number as a string
    let digits = '';
    for (let i = 0; i < 9; i++) {
        digits += Math.floor(Math.random() * 10); // Generates a single digit (0-9)
    }
    return 'ttf' + digits; // Prefix with 'ttf' and return
}

const parseCSV = async (csvFilePath) => { // Make function async
    let films = [];

    // Read the CSV data from the file
    const data = readFileSync(csvFilePath, 'utf8');
    
    // Parse the CSV data
    const records = parse(data, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        cast: (value, context) => {
            if (context.column === 'Hours Viewed' || context.column === 'Views') {
                return parseInt(value.replace(/,/g, ''), 10);
            }
            return value;
        }
    });

    // Process each record
    for (let record of records) { // Use for-loop to properly await within the loop
        const runtime = record.Runtime;
        const runtimeParts = runtime.split(':');
        const runtimeSeconds = parseInt(runtimeParts[0], 10) * 3600 + parseInt(runtimeParts[1], 10) * 60;
        const cleanTitle = record.Title.split('//')[0].replace(/\(\d+\)/g, '').trim();

        // Create the film object
        let film = {
            meta: {
                title: cleanTitle,
                runtime: runtimeSeconds,
            },
            data: {
                raw: {
                    uuid: await createUUID(), // Properly handle the async UUID creation
                    rawTitle: record.Title,
                    availGlobal: record['Available Globally?'] === 'Yes',
                    releaseDate: record['Release Date'] || null,
                    hoursViewed: record['Hours Viewed'],
                    runTime: runtimeSeconds,
                    views: record['Views'],
                },
                clean: {
                    cleanTitle: cleanTitle,
                }
            }
        };

        // Add the film object to the array
        films.push(film);
    }

    return films;
};

const saveToJSONL = async (films, jsonlFilePath) => {
    // Convert the films array to a JSONL string
    const jsonlData = films.map(film => JSON.stringify(film)).join('\n');

    // Write the JSONL data to the file
    writeFileSync(jsonlFilePath, jsonlData);
};

const main = async () => {
    let csvFilePath = './data/netflixRawData-Film.csv';
    let films = await parseCSV(csvFilePath); // Await the parsing function since it's now async
    console.log(films); // Print the films array to verify data
    await saveToJSONL(films, './data/rawfilms.jsonl');
};

main();