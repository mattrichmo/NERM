import axios from 'axios';
import cheerio from 'cheerio';
import { readFileSync, writeFileSync } from 'fs';
import chalk from 'chalk';



const saveToJSONL = async (batch, jsonlFilePath) => {
    // Convert each film in the batch to a JSON string and join with newline
    const jsonlData = batch.map(film => JSON.stringify(film)).join('\n') + '\n'; // Ensure each batch ends with a newline
    writeFileSync(jsonlFilePath, jsonlData, { flag: 'a' }); // Append to the file with flag 'a'
};

const timeout = (ms) => new Promise(resolve => setTimeout(resolve, ms));


// Common user agents
const userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36'
];




const loadBatches = async (batchSize) => {
    let batches = [];
    // Read the file and split by newline, then filter out any empty lines
    let films = readFileSync('./data/processedfilms.jsonl', 'utf8')
        .split('\n')
        .filter(line => line.trim()) // Filter out empty lines
        .map(line => JSON.parse(line)); // Parse each line as JSON
        // filter out lines with imdbid null
        films = films.filter(film => film.meta.id !== null);

    for (let i = 0; i < films.length; i += batchSize) {
        let batch = films.slice(i, i + batchSize);
        batches.push(batch);
    }

    return batches;
}
const getFilmDetails = async (imdbID) => {
    let detailsUrl = `https://www.imdb.com/title/${imdbID}/`;

    let details = {
        img: {
            url: null,
        },
        description: null,
        genres: [],
        popularity: null,
    };

    try {
        const userAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
        const response = await axios.get(detailsUrl, {
            headers: {
                'User-Agent': userAgent
            }
        });

        const $ = cheerio.load(response.data);

        // Get the film cover image
        details.img.url = $('.ipc-media--poster-l img').attr('src');
        details.description = $('p.sc-cafe919b-3').text();
        details.genres = $('.sc-1f50b7c-4 div.ipc-chip-list__scroller span').map((i, el) => $(el).text()).get();
        details.popularity = $('div.sc-5f7fb5b4-1').text();


        console.log(chalk.green(`Description: ${details.description}`));
        console.log(chalk.green(`Genres: ${details.genres.join(', ')}`));
        console.log(chalk.green(`Popularity: ${details.popularity}`));
    


        return details;
    }
    catch (error) {
        console.error('Failed to fetch or parse IMDb details:', error);
    }
}

const getFilmRatings = async (imdbID) => {
    let ratingsUrl = `https://www.imdb.com/title/${imdbID}/ratings/`;

    let ratings = {
        avgRating: null,
        numRatings: null,
    };

    try {
        const userAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
        const response = await axios.get(ratingsUrl, {
            headers: {
                'User-Agent': userAgent
            }
        });

        const $ = cheerio.load(response.data);

        ratings.avgRating = $('span.sc-5931bdee-1').text();
        ratings.numRatings = $('div.sc-5931bdee-3').text();

        console.log(chalk.green(`Average rating: ${ratings.avgRating} / 10 | Number of ratings: ${ratings.numRatings}`));

        return ratings;
    }
    catch (error) {
        console.error('Failed to fetch or parse IMDb ratings:', error);
    }
};
        
const getFilmCrew = async (imdbID) => {
    let crew = {
        departments: []
    };

    const crewUrl = `https://www.imdb.com/title/${imdbID}/fullcredits/`;

    try {
        const userAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
        const response = await axios.get(crewUrl, {
            headers: {
                'User-Agent': userAgent
            }
        });

        const $ = cheerio.load(response.data);
// Improved handling for the cast table
const castTable = $('#fullcredits_content > table.cast_list');

if (castTable.length > 0) {
    let castMembers = [];
    castTable.find('tr').each(function () {
        const row = $(this);
        const imgSrc = row.find('td.primary_photo img').attr('src') || ''; // Default to empty string if src not found
        const nameLink = row.find('td:nth-child(2) a'); // Adjusted selector for name
        const actorName = nameLink.text().trim();
        const actorHref = nameLink.attr('href');
        const actorIdMatch = actorHref ? actorHref.match(/name\/(nm\d+)/) : null;
        const actorId = actorIdMatch ? actorIdMatch[1] : '';

        const characters = [];
        row.find('td.character a').each(function() {
            const characterLink = $(this);
            const characterName = characterLink.text().trim();
            const characterHref = characterLink.attr('href');
            const characterIdMatch = characterHref ? characterHref.match(/nm\d+/) : null;
            const characterId = characterIdMatch ? characterIdMatch[0] : '';
            characters.push({
                name: characterName,
                id: characterId // Capture character ID from link
            });
        });

        if (nameLink.length > 0 && characters.length > 0) {
            castMembers.push({
                name: actorName,
                id: actorId,
                img: imgSrc,
                characters: characters,
                role: 'cast' // Assuming 'cast' as the role for all members
            });
        }
    });

    if (castMembers.length > 0) {
        crew.departments.push({
            department: 'Cast',
            members: castMembers
        });
    }
}

        // Handling other departments
        $('h4').each(function () {
            const departmentID = $(this).attr('id');
            if (departmentID && departmentID !== 'cast') { // Skip 'cast' since it's handled separately
                const departmentName = departmentID.charAt(0).toUpperCase() + departmentID.slice(1);
                const membersTable = $(this).next('table:not(.cast_list)'); // Select non-cast tables

                let members = [];
                membersTable.find('tbody tr').each(function () {
                    const nameLink = $(this).find('td.name a');
                    if (nameLink.length > 0) {
                        const name = nameLink.text().trim();
                        const href = nameLink.attr('href');
                        const idMatch = href && href.match(/name\/(nm\d+)/);
                        if (idMatch) {
                            const id = idMatch[1];
                            const role = $(this).find('td.credit').text().trim();
                            members.push({
                                name: name,
                                id: id,
                                role: role
                            });
                        }
                    }
                });

                if (members.length > 0) {
                    crew.departments.push({
                        department: departmentName,
                        members: members
                    });
                }
            }
        });

        // now console log the number of each department key in a nice chalk design 
        crew.departments.forEach(department => {
            console.log(chalk.blue(`${department.department}: ${department.members.length}`));
        }); 

        return crew;
    } catch (error) {
        console.error('Failed to fetch crew data:', error);
        throw error;
    }
};

const getFilmCompanies = async (imdbID) => {
    let companies = {
        production: [],
        distribution: [],
        effects: [],
        other: [],
    };

    const companiesUrl = `https://www.imdb.com/title/${imdbID}/companycredits/`;

    try {
        const userAgent = userAgents[Math.floor(Math.random() * userAgents.length)];
        const response = await axios.get(companiesUrl, {
            headers: {
                'User-Agent': userAgent
            }
        });

        // Assuming response.data contains the HTML of the company credits page
        const $ = cheerio.load(response.data);

        // Define sections with their corresponding selectors
        const sections = {
            production: "div[data-testid='sub-section-production']",
            distribution: "div[data-testid='sub-section-distribution']", // Adjust this selector as per actual ID
            effects: "div[data-testid='sub-section-specialEffects']",
            other: "div[data-testid='sub-section-miscellaneous']"
        };

        Object.keys(sections).forEach(section => {
            $(sections[section] + ' ul.ipc-metadata-list li').each((index, element) => {
                const companyName = $(element).find('.ipc-metadata-list-item__label').text().trim();
                const companyHref = $(element).find('.ipc-metadata-list-item__label').attr('href');
                const companyIdMatch = companyHref ? companyHref.match(/\/company\/(co\d+)/) : null;
                const companyId = companyIdMatch ? companyIdMatch[1] : '';

                if (companyName && companyId) {
                    companies[section].push({ name: companyName, id: companyId });
                }
            });

            // Null out empty arrays
            if (companies[section].length === 0) {
                companies[section] = null;
            }
        });

        // consoel log the number in each category on the same line 
        console.log(chalk.yellow(`Production: ${companies.production ? companies.production.length : 0} | Distribution: ${companies.distribution ? companies.distribution.length : 0} | Effects: ${companies.effects ? companies.effects.length : 0} | Other: ${companies.other ? companies.other.length : 0}`));

        //console.log(JSON.stringify(companies, null, 2));
        return companies;

    } catch (error) {
        console.error('Failed to fetch company data:', error);
        throw error;
    }
};





const getIMDBData = async (batch) => {
    const concurrentRequests = 1; // Adjust the number of concurrent requests as needed
    const chunkSize = concurrentRequests;
    for (let i = 0; i < batch.length; i += chunkSize) {
        const chunk = batch.slice(i, i + chunkSize);
    await Promise.all(chunk.map(async (film) => {
        console.log(chalk.cyan(`Processing film: ${film.meta.title}`));

        film.meta.scores = {};

        let imdbID = film.meta.id;

        // Launch all asynchronous operations simultaneously
        const [details, ratings, crew, companies] = await Promise.all([
            getFilmDetails(imdbID),
            getFilmRatings(imdbID),
            getFilmCrew(imdbID),
            //getFilmReviews(imdbID),
            getFilmCompanies(imdbID)
        ]);

        console.log(chalk.dim(`Details: ✓`));
        console.log(chalk.dim(`Ratings: ✓`));
        console.log(chalk.dim(`Crew: ✓`));
        // Additional logs for other operations can be added here

        // Update film object with the results
        film.meta.description = details.description;
        film.meta.genres = details.genres;
        film.meta.img = details.img.url;
        film.meta.scores.popularity = details.popularity;
        film.meta.scores.avgRating = ratings.avgRating;
        film.meta.scores.numRatings = ratings.numRatings;
        film.crew = crew; 
        film.companies = companies;
    }));
    }
    saveToJSONL(batch, './data/scrapedfilms.jsonl');
}

const main = async () => {

    let batchSize = 10;
    let batches = await loadBatches(batchSize);
    
    for (let i = 0; i < batches.length; i++) {
        let batch = batches[i];
        console.log(chalk.magenta(`Processing batch ${i + 1} of ${batches.length}.`));
        await getIMDBData(batch);
        // Wait for 3 seconds before processing the next batch
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

}

main();



