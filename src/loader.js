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
  "mongodb+srv://bogdandragomirgeorge:bEfNfE1qoLmiNH4y@cluster0.bioh4ut.mongodb.net/?retryWrites=true&w=majority";
// const mongoUri =
//   "mongodb+srv://bogdandragomirgeorge:bEfNfE1qoLmiNH4y@cluster0.bioh4ut.mongodb.net/?retryWrites=true&w=majority";
const mongoClient = new MongoClient(mongoUri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    depreceationErrors: true,
  },
});

// async function pingMongo() {
//   try {
//     await mongoClient.connect();
//     //serverStatus[0] = true;
//     await mongoClient.db("admin").command({ ping: 1 });
//     console.log("Pinged scalableMongo");
//   } finally {
//     await mongoClient.close();
//   }
// }

// push
// constant that define queries
// run the queries
// signle read query 1000 times
// single write query 1000 times
// aggregation query 100 times
// find everyone who follows who follows this user (find all mutual friends)

const runMongo = async (nodeFilePath, edgesFilePath) => {
  uploadUsersMongo(mongoClient, nodeFilePath);
  uploadEdgesUsersMongo(mongoClient, edgesFilePath)
    .then(() => {
      console.log("Mongo :: Graph Data Loaded successfully");
      mongoClient.close();
    })
    .catch((err) => [console.log("Mongo :: Error Loading Graph Data: ", err)]);
};

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

// runMongo(nodeFilePath, edgesFilePath);

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

// pingMongo().catch(console.dir);

//runMongo(nodeFilePath, edgesFilePath);

// LOAD ARANGO GRAPH

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

let serverStatus = [arangoClient, mongoClient];

const timeQuery = (query, databaseType) => {
  console.time("Query Timer");

  return console.timeEnd("QUery Timer");
};

console.log("MongoDB vs ArangoDB: The grand showdown!");

// build the graph on arangoDB
const buildArangoGraph = async (nodeFilePath, edgesFilePath) => {
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

// buildArangoGraph(nodeFilePath, edgesFilePath);

const singleReadQuery = async (client, collectionName) => {
  // the FOR loop in AQL expects an iterable like a collection or array. We thus first create a collection object
  let collection = client.collection(collectionName);
  const singleReadQuery = await client.query(aql`
    FOR doc IN ${collection}
    FILTER doc.age > 18 && doc.age < 28
    LIMIT 1000
    RETURN doc
  `);

  // const doc = await singleReadQuery.all()
  // console.log(doc)

  await singleReadQuery.all();
};

const singleReadMongo = async (client, collectionName) => {
  let database = client.db("users");
  let collection = database.collection(collectionName);

  const singleReadQuery = await collection
    .find({ age: { $gt: 18, $lt: 28 } })
    .limit(1000)
    .toArray();

  console.log(singleReadQuery);
};

const singleWriteMongo = async (client, collectionName) => {
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

const aggregationQueryMongo = async (client, collectionName) => {
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

const distinctNeighbourSecondOrderQueryMongo = async (
  client,
  collectionName
) => {
  let database = client.db("users");
  let collection = database.collection(collectionName);
  let collectionEdges = database.collection("usersEdgesData");

  const getUsers = await collection.find({ age: { $gt: -1 } }).toArray();

  let user = await getUsers[Math.floor(Math.random() * getUsers.length)];

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

  console.log(mutualFollowers);
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

// do the same for a single read query on mongo

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

const singleWriteQuery = async (client, collectionName) => {
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

const aggregationQuery = async (client, collectionName) => {
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

const distinctNeighbourSecondOrderQuery = async (client, collectionName) => {
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
  // console.log(mutualFollowers)
};

/***
 * BENCHMARK QUERIES
 */
// singleReadQuery(arangoClient, "fakeSocialMediaWithAge")
// singleWriteQuery(arangoClient, "fakeSocialMediaWithAge")
// aggregationQuery(arangoClient, "fakeSocialMediaWithAge")

async function measureExecutionTime(functionToTime, functionName, args) {
  const startTime = performanceNow();

  await functionToTime(...args);

  const endTime = performanceNow();

  const executionTime = endTime - startTime;

  console.log(`${functionName} : ${executionTime} milliseconds`);
  return executionTime;
}

const averageTimeReadMongo = async (mongoClient, collectionName) => {
  let totalTime = 0;

  for (let i = 0; i < 1000; i++) {
    let executionTime = await measureExecutionTime(
      singleReadMongo,
      "Single Read Query Mongo",
      [mongoClient, collectionName]
    );

    totalTime += executionTime;
  }

  mongoClient.close();
  return totalTime / 1000;
};

const averageTimeWriteMongo = async (mongoClient, collectionName) => {
  let totalTime = 0;

  for (let i = 0; i < 1000; i++) {
    let executionTime = await measureExecutionTime(
      singleWriteMongo,
      "Single Write Query Mongo",
      [mongoClient, collectionName]
    );

    totalTime += executionTime;
  }

  mongoClient.close();
  return totalTime / 1000;
};

const averageTimeAggregationMongo = async (mongoClient, collectionName) => {
  let totalTime = 0;

  for (let i = 0; i < 250; i++) {
    let executionTime = await measureExecutionTime(
      aggregationQueryMongo,
      "Aggregation Query Mongo",
      [mongoClient, collectionName]
    );

    totalTime += executionTime;
  }

  mongoClient.close();
  return totalTime / 250;
};

const averageTimeDistinctNeighbourSecondOrderMongo = async (
  mongoClient,
  collectionName
) => {
  let totalTime = 0;

  for (let i = 0; i < 250; i++) {
    let executionTime = await measureExecutionTime(
      distinctNeighbourSecondOrderQueryMongo,
      "Distinct Neighbour Second Order Mongo",
      [mongoClient, collectionName]
    );

    totalTime += executionTime;
  }

  mongoClient.close();
  return totalTime / 250;
};

// averageTimeReadMongo(mongoClient, "usersData").then((averageTime) => {
//   console.log("Average Execution Time For Read Query: " + averageTime);
// }); // after some runs --> 67.82984070008993

// averageTimeWriteMongo(mongoClient, "usersData").then((averageTime) => {
//   console.log("Average Execution Time For Write Query: " + averageTime);
// }); // after a run --> 3004.8619921003283

// averageTimeAggregationMongo(mongoClient, "usersData").then((averageTime) => {
//   console.log("Average Execution Time For Aggregation Query: " + averageTime);
// }); // after some runs -->  31.546256799817087

// averageTimeDistinctNeighbourSecondOrderMongo(mongoClient, "usersData").then(
//   (averageTime) => {
//     console.log(
//       "Average Execution Time For Distinct Neighbour Second Order Query: " +
//         averageTime
//     );
//   }
// ); // after some run --> 318.701121199131

// single read and write 1000 times
// aggregation query 250
// distinct 250

// const arangoFunctions = {
//   "Load Graph": [buildArangoGraph, [nodeFilePath, edgesFilePath]],
//   "Single Read Query": [
//     singleReadQuery,
//     [arangoClient, "fakeSocialMediaWithAge"],
//   ],
//   "Single Write Query": [
//     singleWriteQuery,
//     [arangoClient, "fakeSocialMediaWithAge"],
//   ],
//   "Aggregation Query": [
//     aggregationQuery,
//     [arangoClient, "fakeSocialMediaWithAge"],
//   ],
//   "Distinct Neighbours Second Order": [
//     distinctNeighbourSecondOrderQuery,
//     [arangoClient, "fakeSocialMediaWithAge"],
//   ],
// };

// const mongoFunctions = {
//   "Single Read Query Mongo": [singleReadMongo, [mongoClient, "usersData"]],
//   "Single Write Query Mongo": [singleWriteMongo, [mongoClient, "usersData"]],
//   "Aggregation Query Mongo": [
//     aggregationQueryMongo,
//     [mongoClient, "usersData"],
//   ],
//   "Distinct Neighbours Second Order Mongo": [
//     distinctNeighbourSecondOrderQueryMongo,
//     [mongoClient, "usersData"],
//   ],
// };

// Object.keys(mongoFunctions).forEach((metric) => {
//   console.log(mongoFunctions[metric]);
// });

// // Object.keys(arangoFunctions).forEach((metric) => {
//   console.log(arangoFunctions[metric]);
//   measureExecutionTime(
//     arangoFunctions[metric][0],
//     metric,
//     arangoFunctions[metric][1]
//   );
// });
