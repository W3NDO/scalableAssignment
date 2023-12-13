const { MongoClient, ServerApiVersion } = require("mongodb");
const arangojs = require("arangojs");
const { aql } = require("arangojs/aql");
const fs = require("fs");
var path = require("path");
const { parse } = require("csv-parse");
const { finished } = require("stream/promises");
const performanceNow = require("performance-now");

let nodeFilePath = path.join(__dirname, "datasets/fake_users2.csv");
let edgesFilePath = path.join(__dirname, "datasets/fake_social_media2.csv");

/***
 * ARANGO Client Setup
 */

const {
  Graph,
  GraphEdgeCollection,
  GraphVertexCollection,
} = require("arangojs/graph");

/***
 * Arango Client Setup
 */
const arangoClient = arangojs({
  url: "http://localhost:8529",
  databaseName: "TestArango",
  auth: { username: "root", password: "pswd1" },
});

/***
 * MongoClient Setup
 */
const mongoUri =
  "mongodb://mongoadmin:pswd1@localhost:27888";
// const mongoUri =
//   "mongodb+srv://bogdandragomirgeorge:bEfNfE1qoLmiNH4y@cluster0.bioh4ut.mongodb.net/?retryWrites=true&w=majority";
const mongoClient = new MongoClient(mongoUri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    depreceationErrors: true,
  }, 
});

// reads in a csv file and return an interable array of objects
async function loadDataAsObjects(filePath) {
  const records = [];
  const parser = fs.createReadStream(filePath).pipe(
    parse({
      // CSV options if any
    })
  );
  parser.on("readable", function () {
    let record;
    while ((record = parser.read()) !== null) {
      // Work with each record
      records.push(record);
    }
  });
  await finished(parser);
  return records;
}


/***
 * Load Graph on Mongo
 */
async function uploadUsersMongo(mongoClient, nodeFilePath) {
  const database = mongoClient.db("users");
  const usersCollection = database.collection("usersData");

  var records = await loadDataAsObjects(nodeFilePath);
  var user = {};
  for (const record of records) {
    const [name, age, username] = record;
    if (name === "name") {
      continue;
    }
    user = {
      name: name,
      age: Number(age),
      username: username,
    };
    const p = await usersCollection.insertOne(user);
    //console.log([name, age, username]);
  }

}

async function uploadEdgesUsersMongo(mongoClient, edgesFilePath) {
  const database = mongoClient.db("users");
  const usersEdgesCollection = database.collection("usersEdgesData");

  var records = await loadDataAsObjects(edgesFilePath);
  var edge = {};
  for (const record of records) {
    const [edgePredicate, sourceNode, destinationNode] = record;
    if (edgePredicate === "predicate") {
      continue;
    }
    edge = {
      predicate: edgePredicate,
      user1: sourceNode,
      user2: destinationNode,
    };
    const p = await usersEdgesCollection.insertOne(edge);
  }
}

const mongoLoadGraph = async (nodeFilePath, edgesFilePath) => {
  await uploadUsersMongo(mongoClient, nodeFilePath);
  await uploadEdgesUsersMongo(mongoClient, edgesFilePath)
    .then(() => {
      console.log("Mongo :: Graph Data Loaded successfully");
      // mongoClient.close();
    })
    .catch((err) => [console.log("Mongo :: Error Loading Graph Data: ", err)]);
};


/***
 * Load Graph on Arango
 */
// inserts documents into a collection
async function arangoLoadNodes(client, filePath) {
  var nodes = await loadDataAsObjects(filePath);
  for (const node of nodes) {
    var [name, age, username] = node;
    var nodeInsertQuery;
    if (true) {
      nodeInsertQuery = aql`
        UPSERT {
          _key: ${username},
          name: ${name},
          age: ${Number(age)}
        }
        INSERT {
          _key: ${username},
          name: ${name},
          age: ${Number(age)}
        }
        UPDATE {}
        IN "fakeSocialMediaWithAge"
      `;
    }
    await client.query(nodeInsertQuery);
  }
}

// inserts edge definition documents into an edge collection
async function arangoLoadEdges(
  client,
  filePath,
  nodeCollectionName,
  edgeCollectionName
) {
  var records = await loadDataAsObjects(filePath);
  for (const record of records) {
    const [edge, srcNode, destNode] = record;
    if (srcNode === "subject") {
      continue;
    }
    // console.log([edge, srcNode, destNode]);
    var edgeInsertQuery = {
      query:
        "UPSERT { _from: @sourceNode, _to: @destNode, edge: {name: @edgeName} } INSERT { _from: @sourceNode, _to: @destNode, edge: {name: @edgeName }} UPDATE {} IN @@edgeCollection ",
      bindVars: {
        "@edgeCollection": edgeCollectionName,
        sourceNode: `${nodeCollectionName}/${srcNode}`,
        destNode: `${nodeCollectionName}/${destNode}`,
        edgeName: "follows",
      },
    };

    await client.query(edgeInsertQuery);
  }
}

// build the graph on arangoDB
const arangoLoadGraph = async (nodeFilePath, edgesFilePath) => {
  let nodeInsertEndTime, edgeInsertEndTime;
  arangoLoadNodes(arangoClient, nodeFilePath).then(() => {
    nodeInsertEndTime = performance.now();
  });
  arangoLoadEdges(
    arangoClient,
    edgesFilePath,
    "fakeSocialMediaWithAge",
    "fakeSocialMediaWithAgeEdges"
  )
    .then(() => {
      console.log("ARANGO :: Graph Data Loaded successfully");
    })
    .catch((err) => [console.log("ARANGO :: Error Loading Graph Data: ", err)]);
};

/***
 * Queries
 */

// Single Read
const arangoSingleReadQuery = async (client, collectionName) => {
  // the FOR loop in AQL expects an iterable like a collection or array. We thus first create a collection object
  let collection = client.collection(collectionName);
  const singleReadQuery = await client.query(aql`
    FOR doc IN ${collection}
    FILTER doc.age > 18 && doc.age < 28
    LIMIT 1000
    RETURN doc
  `);

  await singleReadQuery.all();
};

const mongoSingleReadQuery = async (client, collectionName) => {
  let database = client.db("users");
  let collection = database.collection(collectionName);

  const singleReadQuery = await collection
    .find({ age: { $gt: 18, $lt: 28 } })
    .limit(1000)
    .toArray();

  return singleReadQuery;
};

// single write queries
const mongoSingleWriteQuery = async (client, collectionName) => {
  let database = client.db("users");
  let collection = database.collection(collectionName);
  let collectionEdges = database.collection("usersEdgesData");

  // as an example, we would want to find the first ten GenZ users (below 24) and make them mildly famous.

  const getGenZUsersQuery = await collection
    .find({ age: { $lt: 24 } })
    .limit(1000)
    .toArray();

  let popularUser = await getGenZUsersQuery[
    Math.floor(Math.random() * getGenZUsersQuery.length)
  ];

  // select all the genZ names
  const addUser = await getGenZUsersQuery.map((user) => {
    collectionEdges.insertOne({
      predicate: "follows",
      user1: user.username,
      user2: popularUser.username,
    });
  });

  return addUser;
};

const arangoSingleWriteQuery = async (client, collectionName) => {
  // as an example, we would want to find the first ten GenZ users (below 24) and make them mildly famous.
  let collection = client.collection(collectionName);
  const getGenZUsersQuery = await client.query(aql`
    FOR user IN ${collection}
    FILTER user.age < 24
    LIMIT 10
    RETURN user
  `);
  const genZ = await getGenZUsersQuery.all();

  let popularUser = await genZ[Math.floor(Math.random() * genZ.length)];
  // get a random user to make popular

  // This query makes every other genZ person follow our randomly selected popular user
  const makeFamousQuery = await client.query(aql`
    FOR genZ IN ${genZ}
      UPSERT {
        _from: genZ._id,
        _to: ${popularUser._id},
        edge: {
          name: "follows"
        }
      } INSERT {
        _from: genZ._id,
        _to: ${popularUser._id},
        edge: {
          name: "follows"
        }
      } UPDATE {}
      IN "fakeSocialMediaWithAgeEdges"
  `);

  await makeFamousQuery.next();
};

// aggregation queries
const mongoAggregationQuery = async (client, collectionName) => {
  const database = client.db("users");
  const collection = database.collection(collectionName);

  const ageDistributionQuery = await collection
    .aggregate([
      {
        $match: {
          age: { $gt: -1 },
        },
      },
      {
        $group: {
          _id: "$age",
          age: { $first: "$age" },
          count: { $sum: 1 },
        },
      },
    ])
    .toArray();

  return ageDistributionQuery;
};

const arangoAggregationQuery = async (client, collectionName) => {
  // For aggregation we will compute the age distribution of our users for marketing purposes
  let collection = client.collection(collectionName);
  let ageDistributionQuery = await client.query(aql`
    FOR u IN ${collection}
    COLLECT age = u.age WITH COUNT INTO length
    RETURN { 
      "age" : age, 
      "count" : length 
    }
  `);
  // let ageDistribution = await ageDistributionQuery.all()
  // return ageDistribution
  await ageDistributionQuery.all();
};

// distinct neighbours of second order query
const mongoDistinctNeighbourSecondOrderQuery = async (
  client,
  collectionName
) => {
  let database = client.db("users");
  let collection = database.collection(collectionName);
  let collectionEdges = database.collection("usersEdgesData");

  let user = await collection.findOne({ username: "Genie_94" });

  let followersOfUser = await collectionEdges
    .find({ user2: user.username })
    .toArray();

  let followersOfUserNames = [];

  for (const follower of followersOfUser) {
    let followerDetails = await collection.findOne({
      username: follower.user1,
    });
    followersOfUserNames.push(followerDetails.name);
  }

  //console.log(followersOfUser);
  let mutualFollowers = [];
  //console.log(mutualFollowers);
  //console.log(followersOfUserNames);

  for (const follower of followersOfUser) {
    let mutualFollowersQuery = await collectionEdges
      .find({ user1: follower.user1 })
      .toArray();

    for (const mutualFollower of mutualFollowersQuery) {
      let mutualFollowerDetails = await collection.findOne({
        username: mutualFollower.user2,
      });

      if (!mutualFollowers.includes(mutualFollowerDetails.name)) {
        mutualFollowers.push(mutualFollowerDetails.name);
      }
    }
  }

  return mutualFollowers;
};

const arangoDistinctNeighbourSecondOrderQuery = async (client, collectionName) => {
  // given a user, find all their followers and the mutual followers.
  let collection = client.collection(collectionName);
  let user = await collection.document("Bogdan_22");
  let mutualFollowersQuery = await client.query(aql`
  FOR vertex IN 2..2 OUTBOUND ${user._id}
    GRAPH 'fakeSMGraph'
    OPTIONS{parallelism:8,order:'dfs'}
    RETURN vertex.name
  `); // graph traversal queries allow for parallelism.

  let mutualFollowers = await mutualFollowersQuery.all();
  return mutualFollowers
};



// distinctNeighbourSecondOrderQueryMongo(mongoClient, "usersData").then(() => {
//   console.log("Mongo :: Distinct Neighbour Second Order Query Completed");
//   mongoClient.close();
// });

// singleReadMongo(mongoClient, "usersData").then(() => {
//   console.log("Mongo :: Single Read Query Completed");
// });

// aggregationQueryMongo(mongoClient, "usersData").then(() => {
//   console.log("Mongo :: Aggregation Query Completed");
// });

// singleWriteMongo(mongoClient, "usersData").then(() => {
//   console.log("Mongo :: Single Write Query Completed");
// });


// singleWriteMongo(mongoClient, "usersData").then(() => {
//   console.log("Mongo :: Single Write Query Completed");
//   //mongoClient.close();
// });

// Promise.all([
//   singleReadMongo(mongoClient, "usersData").then(() => {
//     console.log("Mongo :: Single Read Query Completed");
//   }),
//   aggregationQueryMongo(mongoClient, "usersData").then(() => {
//     console.log("Mongo :: Aggregation Query Completed");
//   }),
//   singleWriteMongo(mongoClient, "usersData").then(() => {
//     console.log("Mongo :: Single Write Query Completed");
//   }),
// ]);

// async function closeMongo() {
//   await mongoClient.close();
// }

// Promise.all([
//   closeMongo().then(() => {
//     console.log("Mongo :: Connection Closed");
//   }),
// ]);


/***
 * BENCHMARK QUERIES
 */

async function measureExecutionTime(functionToTime, functionName, args) {
  const startTime = performanceNow();

  let result = await functionToTime(...args);

  let endTime, executionTime;

  await functionToTime(...args).then( () => {
    endTime = performanceNow()
    executionTime = endTime - startTime;
  })

  console.log(`${functionName} : ${executionTime} milliseconds`);
  return [functionName, executionTime]
}

// single read and write 1000 times
// aggregation query 250
// distinct 250

const arangoFunctions = {
  "Arango Load Graph": [arangoLoadGraph, [nodeFilePath, edgesFilePath]],
  "Arango Single Read Query": [
    arangoSingleReadQuery,
    [arangoClient, "fakeSocialMediaWithAge"],
  ],
  "Single Write Query": [
    arangoSingleWriteQuery,
    [arangoClient, "fakeSocialMediaWithAge"],
  ],
  "Arango Aggregation Query": [
    arangoAggregationQuery,
    [arangoClient, "fakeSocialMediaWithAge"],
  ],
  "Arango Distinct Neighbours Second Order": [
    arangoDistinctNeighbourSecondOrderQuery,
    [arangoClient, "fakeSocialMediaWithAge"],
  ],
};

const mongoFunctions = {
  "Mongo Load Graph": [mongoLoadGraph, [nodeFilePath, edgesFilePath]],
  "Mongo Single Read Query Mongo": [mongoSingleReadQuery, [mongoClient, "usersData"]],
  "Mongo Single Write Query Mongo": [mongoSingleWriteQuery, [mongoClient, "usersData"]],
  "Mongo Aggregation Query Mongo": [
    mongoAggregationQuery,
    [mongoClient, "usersData"],
  ],
  "Mongo Distinct Neighbours Second Order Mongo": [
    mongoDistinctNeighbourSecondOrderQuery,
    [mongoClient, "usersData"],
  ],
};

let metrics = {}

Object.keys(arangoFunctions).forEach(async (metric) => {
  // console.log(arangoFunctions[metric]);
  metrics[metric] = metrics[metric] || []
  metrics[metric] = measureExecutionTime(
    arangoFunctions[metric][0],
    metric,
    arangoFunctions[metric][1]
  )[1]
});

Object.keys(mongoFunctions).forEach(async (metric) => {
  console.log(mongoFunctions[metric]);
  metrics[metric] = metrics[metric] || []
  metrics[metric] = measureExecutionTime(
    mongoFunctions[metric][0],
    metric,
    mongoFunctions[metric][1]
  )[1]
});


function add(accumulator, a){
  return accumulator + a
}
