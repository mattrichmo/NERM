import axios from 'axios';
import cheerio from 'cheerio';
import { readFileSync, writeFileSync, existsSync } from 'fs';
import levenshtein from 'fast-levenshtein';
import chalk from 'chalk';


// Function to save batch to JSONL file
const saveToJSONL = async (batch, jsonlFilePath) => {
    for (let i = 0; i < batch.length; i++) {
        let film = batch[i];
        let line = JSON.stringify(film);
        writeFileSync(jsonlFilePath, `${line}\n`, { flag: 'a' });
    }
};

// Function to load batches of films with filtering based on already processed films
const loadBatches = async (batchSize) => {
    let batches = [];
    let rawData;
    try {
        rawData = readFileSync('./data/rawfilms.jsonl', 'utf8');
    } catch (error) {
        console.log(chalk.red('Error reading raw films file.'));
        throw error; // Rethrow to handle it outside or log it.
    }
    const rawLines = rawData.split('\n').filter(line => line.trim());

    // Create a new set of processed IDs. If the file doesn't exist, log that no processed films were found.
    let processedIDs = new Set();
    if (existsSync('./data/processedFilms.jsonl')) {
        const processedData = readFileSync('./data/processedFilms.jsonl', 'utf8');
        const processedLines = processedData.split('\n').filter(line => line.trim());
        processedIDs = new Set(processedLines.map(line => JSON.parse(line).data.raw.uuid));
        console.log(chalk.yellow(`Processed films found: ${processedIDs.size}`));
    } else {
        console.log(chalk.yellow('No processed films file found. Assuming starting from zero.'));
    }

    // Create the batch of films and filter out the processed films by matching the processed IDs and the raw IDs
    let batch = [];
    let rawFilms = rawLines.map(line => JSON.parse(line));
    for (let film of rawFilms) {
        if (!processedIDs.has(film.data.raw.uuid)) {
            batch.push(film);
            if (batch.length >= batchSize) {
                batches.push(batch);
                batch = [];
            }
        }
    }
    // If there's a remaining batch that hasn't reached the batchSize but still has films, add it to batches
    if (batch.length > 0) {
        batches.push(batch);
    }

    console.log(chalk.green(`Total batches created: ${batches.length}`));
    return batches;
};

const randomTimeout = () => {

    const min = 3000;
    const max = 5000;

    const timeout = Math.floor(Math.random() * (max - min + 1) + min);
    console.log(chalk.dim(`Waiting for ${timeout}ms before next batch.`));

    return new Promise(resolve => setTimeout(resolve, timeout));
};

// Common user agents
const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36'
];

const parseRuntimeToSeconds = (runtimeStr) => {
    const hoursMatch = runtimeStr.match(/(\d+)h/);
    const minutesMatch = runtimeStr.match(/(\d+)m/);
    const hours = hoursMatch ? parseInt(hoursMatch[1], 10) : 0;
    const minutes = minutesMatch ? parseInt(minutesMatch[1], 10) : 0;
    return hours * 3600 + minutes * 60;
};

const validateSelectedItem = async (validation) => {
    const { og, scraped } = validation;

    // Validate title with a threshold for Levenshtein distance (80% similarity)
    const titleDistance = levenshtein.get(og.title.toLowerCase(), scraped.title.toLowerCase());
    const titleLength = Math.max(og.title.length, scraped.title.length);
    const titleSimilarity = (titleLength - titleDistance) / titleLength;
    if (titleSimilarity < 0.8) {
        console.log(chalk.red(`Validation failed for title: Expected ${og.title}, got ${scraped.title}`));
        return false;
    }

    // Validate runtime within 20% threshold
    const runtimeDiff = Math.abs(og.runtime - scraped.runtime);
    if (runtimeDiff / Math.max(og.runtime, scraped.runtime) > 0.2) {
        console.log(chalk.red(`Validation failed for runtime: Expected ${og.runtime} seconds, got ${scraped.runtime} seconds`));
        return false;
    }

    return true;
};


const searchIMDB = async (title, runtime, releaseYear) => {

    
    console.log(chalk.dim(`Searching IMDb: `),chalk.yellow(title));
    let imdbID = null;
    let url = `https://www.imdb.com/search/title/?title=${encodeURIComponent(title)}`;
    // Select a random user agent from the list
    const userAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
    
    try {
        const response = await axios.get(url, { headers: { 'User-Agent': userAgent } });
        const $ = cheerio.load(response.data);
        const firstItem = $('ul.ipc-metadata-list').first();
        const metadata = $('.sc-b189961a-7.feoqjK.dli-title-metadata').first();
        const releaseYearScraped = metadata.find('span.sc-b189961a-8.kLaxqf.dli-title-metadata-item:nth-child(1)').text();
        const runTimeString = metadata.find('span.sc-b189961a-8.kLaxqf.dli-title-metadata-item:nth-child(2)').text();

        const runtimeSeconds = parseRuntimeToSeconds(runTimeString);

        imdbID = firstItem.find('a.ipc-title-link-wrapper').attr('href').match(/\/title\/(tt\d+)/)[1];

        let validation = { og: { title, runtime, releaseYear }, scraped: { title, runtime: runtimeSeconds, releaseYear: releaseYearScraped } };
        const isValid = await validateSelectedItem(validation);
        if (!isValid) {
            imdbID = null; // Reset imdbID if validation fails
    }

        // console log a checkmark 
        console.log(chalk.green('✓'));

    } catch (error) {
        console.error('Failed to fetch or parse IMDb data:', error);
        imdbID = null;
    }

    return imdbID;
};

// Function to process a batch of film data
const getIMDBData = async (batch) => {
    const concurrentRequests = 5; // Adjust the number of concurrent requests as needed
    const chunkSize = concurrentRequests;
    for (let i = 0; i < batch.length; i += chunkSize) {
        const chunk = batch.slice(i, i + chunkSize);
        await Promise.all(chunk.map(async (film) => {
            let title = film.meta.title;
            let runtime = film.meta.runtime;
            let releaseYear = film.data.raw.releaseDate ? film.data.raw.releaseDate.split('-')[0] : null;
            let imdbID = await searchIMDB(title, runtime, releaseYear);
            film.meta.id = imdbID;

            
        }));
    }
    saveToJSONL(batch, './data/processedFilms.jsonl');
    await randomTimeout();
};


// Main function to execute batch processing
const main = async () => {
    let batchSize = 50;
    let batches = await loadBatches(batchSize);

    for (let i = 0; i < batches.length; i++) {
        let batch = batches[i];
        console.log(chalk.magenta(`Processing batch ${i + 1} of ${batches.length}.`));
        await getIMDBData(batch);
        // Wait for 3 seconds before processing the next batch
        await new Promise(resolve => setTimeout(resolve, 3000));
    }
};
main();