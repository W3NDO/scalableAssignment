const { MongoClient, ServerApiVersion } = require("mongodb");
const arangojs = require("arangojs");
const { aql } = require("arangojs/aql");
const fs = require("fs");
const { parse } = require("csv-parse");
const { finished } = require("stream/promises");

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

const runMongo = async (nodeMongoFilePath, edgesMongoFilePath) => {
  uploadUsersMongo(mongoClient, nodeMongoFilePath);
  uploadEdgesUsersMongo(mongoClient, edgesMongoFilePath)
    .then(() => {
      console.log("Mongo :: Graph Data Loaded successfully");
      mongoClient.close();
    })
    .catch((err) => [console.log("Mongo :: Error Loading Graph Data: ", err)]);
};

async function uploadUsersMongo(mongoClient, nodeMongoFilePath) {
  const database = mongoClient.db("users");
  const usersCollection = database.collection("usersData");

  var records = await loadDataAsObjects(nodeMongoFilePath);
  var user = {};
  for (const record of records) {
    const [name, age, username] = record;
    if (name === "name") {
      continue;
    }
    user = {
      name: name,
      age: age,
      username: username,
    };
    const p = await usersCollection.insertOne(user);
    //console.log([name, age, username]);
  }

  // let nodeMongoFilePath =
  //   "C:/Users/bogda/Desktop/Scalable Data Management Systems/Project/scalableAssignment/src/datasets/fake_users2.csv";
  // let objectData = loadDataAsObjects(nodeMongoFilePath);
}

async function uploadEdgesUsersMongo(mongoClient, edgesMongoFilePath) {
  const database = mongoClient.db("users");
  const usersEdgesCollection = database.collection("usersEdgesData");

  var records = await loadDataAsObjects(edgesMongoFilePath);
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

let nodeMongoFilePath =
  "C:/Users/bogda/Desktop/Scalable Data Management Systems/Project/scalableAssignment/src/datasets/fake_users2.csv";
let edgesMongoFilePath =
  "C:/Users/bogda/Desktop/Scalable Data Management Systems/Project/scalableAssignment/src/datasets/fake_social_media2.csv";
// pingMongo().catch(console.dir);

//uploadUsersMongo(nodeMongoFilePath).catch(console.dir);
//uploadEdgesUsersMongo(edgesMongoFilePath).catch(console.dir);
runMongo(nodeMongoFilePath, edgesMongoFilePath);

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
          age: ${age}
        }
        INSERT {
          _key: ${username},
          name: ${name},
          age: ${age}
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
  const startTime = performance.now()
  let nodeInsertEndTime, edgeInsertEndTime;
  arangoLoadNodes(arangoClient, nodeFilePath).then( () => { nodeInsertEndTime = performance.now()})
  arangoLoadEdges(arangoClient, edgesFilePath, "fakeSocialMediaWithAge" ,"fakeSocialMediaWithAgeEdges")
  .then( ()=> {
    console.log('ARANGO :: Graph Data Loaded successfully')
    edgeInsertEndTime = performance.now()
  })
  .catch( (err) => [
    console.log('ARANGO :: Error Loading Graph Data: ', err )
  ])

  console.log( "Node Insert Time: ", nodeInsertEndTime - startTime)
  console.log( "Node Insert Time: ", edgeInsertEndTime - nodeInsertEndTime)
  console.log( "Node Insert Time: ", edgeInsertEndTime - startTime)
}

nodeFilePath =
  "/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/fake_users2.csv";
edgeDefinitionFilePath =
  "/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/fake_social_media2.csv";

//buildArangoGraph(nodeFilePath, edgeDefinitionFilePath);
