import { MongoClient } from 'mongodb';

const uri = 'mongodb+srv://<username>:<password>@<host>/';
const dbName = 'sample_mflix';
const sourceCollectionName = 'movies';
const client = new MongoClient(uri);

const sourceCollection = client.db(dbName).collection(sourceCollectionName);
const pipeline = [
    {
        $match: {
            countries: "USA",
            rated: {
               $in: ["G","PASSED","APPROVED","TV-14", "GP"]
            },
            released: {
                $gte: new Date("2000-01-01T00:00:00.000Z"),
                $lte: new Date("2024-07-29T02:59:59.999Z")
            }
        }
    },
    {
        $sort: {
            released: 1
        }
    }
];

const calculateMean = arr => arr.reduce((a, b) => a + b) / arr.length;
const findIndexStage = (stage) => {
    if (stage.stage === "IXSCAN") return stage;
    if (stage.stage === "COLLSCAN") return { indexName: "COLLSCAN" };
    if (stage.stage === "SORT_MERGE") return findIndexStage(stage.inputStages[0]);
    if (stage.inputStage) return findIndexStage(stage.inputStage);
    return null;
}

const test = async(options) => {
    const works = [];
    const nReturned = [];
    const time = [];

    let indexName = null;
    let planCacheKey = null;

    // Execute the pipeline 10 times wihtout explain to cache the plan
    const executions = [];
    for(let i = 1; i <= 10; i++){
        executions.push(sourceCollection.aggregate(pipeline, options))
    };

    await Promise.all(executions);
        
    for(let i = 1; i <= 10; i++){
        const result = await sourceCollection.aggregate(pipeline, options).explain("executionStats");
        works.push(result.executionStats.executionStages.works)
        nReturned.push(result.executionStats.executionStages.nReturned)
        time.push(result.executionStats.executionTimeMillis)
        
        if(i === 1){
            planCacheKey = result.queryPlanner.planCacheKey;
            indexName = findIndexStage(result.executionStats.executionStages.inputStage)?.indexName;
        }
    }

    console.log(`
        Used Hint: ${options.hint === undefined ? "No" : "Yes" }
        Works: ${calculateMean(works)}
        nReturned: ${calculateMean(nReturned)}
        TimeMillis: ${calculateMean(time)}
        Index Name: ${indexName}
        Plan Cache Key: ${planCacheKey}`);
    return indexName;
}

(async () => {
    
    console.log('-'.repeat(50));
    const automaticIndex = await test({});

    const hint = automaticIndex == "countries_1_rated_1_released_1"
        ? {"countries": 1, "released": 1}
        : {"countries": 1, "rated": 1, "released": 1};

    console.log('-'.repeat(50) + '\n');
    await test({"hint": hint});
    console.log('-'.repeat(50) + '\n');

    await client.close();
})();
