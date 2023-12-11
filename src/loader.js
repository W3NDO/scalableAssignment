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

const arangoClient = arangojs({
  url: "http://localhost:8529",
  databaseName: "TestArango",
  auth: { username: "root", password: "pswd1" },
});

// const collection = db.collection("anime-ontology");
// const graph = db.graph("testEdgeCollection");
// console.log(graph)
// const collection = graph.edgeCollection("testEdges")
// try {
//   const edge = collection.edge("abc123");
//   console.log(edge);
// } catch (e) {
//   console.error("Could not find edge");
// }

// arangoClient.listCollections()
//   .then(function (collections) {
//     console.log("Your Arango collections: " + collections.map(function (collection) {
//         return collection.name;
//     }).join(", "));
//   });

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

async function pingMongo() {
  try {
    await mongoClient.connect();
    //serverStatus[0] = true;
    await mongoClient.db("admin").command({ ping: 1 });
    console.log("Pinged scalableMongo");
  } finally {
    await mongoClient.close();
  }
}

pingMongo().catch(console.dir);

// /***
//  * Other stuff
//  */

// // LOAD GRAPH

// async function loadDataAsObjects(filePath) {
//   const records = [];
//   const parser = fs.createReadStream(filePath).pipe(
//     parse({
//       // CSV options if any
//     })
//   );
//   parser.on("readable", function () {
//     let record;
//     while ((record = parser.read()) !== null) {
//       // Work with each record
//       records.push(record);
//     }
//   });
//   await finished(parser);
//   return records;
// }

// async function arangoLoadNodes(client, filePath) {
//   var nodes = await loadDataAsObjects(filePath);
//   let [n, iterNodes] = nodes;
//   for (const node of nodes) {
//     var nodeInsertQuery;
//     if (true) {
//       nodeInsertQuery = aql`
//         UPSERT {
//           _key: ${node[0]},
//           name: ${node[0]}
//         }
//         INSERT {
//           _key: ${node[0]},
//           name: ${node[0]}
//         }
//         UPDATE {}
//         IN "fake_social_media"
//       `;
//     }
//     // console.log("QUERY: ", nodeInsertQuery)
//     await client.query(nodeInsertQuery);
//   }
// }

// async function arangoLoadEdges(client, filePath) {
//   var records = await loadDataAsObjects(filePath);
//   let nodes;
//   for (const record of records) {
//     const [edge, srcNode, destNode] = record;
//     if (srcNode === "subject") {
//       continue;
//     }
//     console.log([edge, srcNode, destNode]);
//     var edgeInsertQuery = {
//       query:
//         "UPSERT { _from: @sourceNode, _to: @destNode, edge: {name: @edgeName} } INSERT { _from: @sourceNode, _to: @destNode, edge: {name: @edgeName }} UPDATE {} IN @@edgeCollection ",
//       bindVars: {
//         "@edgeCollection": "fake_social_media_edges",
//         sourceNode: `fake_social_media/${srcNode}`,
//         destNode: `fake_social_media/${destNode}`,
//         edgeName: "follows",
//       },
//     };

//     await client.query(edgeInsertQuery);
//     // let arangoDestNodeID = await client.query(destNodeInsertQuery);
//   }

//   const getSrcDestNodes = async () => {
//     let collectionName = "fake_social_media";
//     var getNodesQuery = {
//       query: "FOR u IN @@collection RETURN u",
//       bindVars: {
//         "@collection": "fake_social_media",
//       },
//     };
//     nodes = await client.query(getNodesQuery);
//     let edgeSrcDest = [];

//     await nodes.forEach(async (n) => {
//       if (n.follows) {
//         edgeSrcDest.push({
//           from: `${collectionName}/${n._key}` || null,
//           edge: "follows",
//           to: `${collectionName}/${n.follows}` || null,
//         });
//       }
//     });
//     return edgeSrcDest;
//   };

//   const buildEdges = async () => {
//     let buildEdgesQuery = async (edgeBuilderObject) => {
//       query = {
//         query:
//           "UPSERT { _from: @sourceNode, _to: @destNode, edge: { name: @edgeName } } INSERT { _from: @sourceNode, _to: @destNode, edge: { name: @edgeName } } UPDATE {} INTO @@edgeCollection ",
//         bindVars: {
//           "@edgeCollection": "fake_social_media_edges",
//           sourceNode: edgeBuilderObject["from"],
//           destNode: edgeBuilderObject["to"],
//           edgeName: edgeBuilderObject["edge"],
//         },
//       };

//       await client.query(query);
//     };

//     const edgeBuilderInfo = await getSrcDestNodes();
//     let queryList = [];
//     edgeBuilderInfo.forEach((user) => {
//       queryList.push(buildEdgesQuery(user));
//     });
//     return queryList;
//   };

//   // console.log( "QUERY : ", await buildEdges())
//   let queries = await buildEdges();
//   // queries.forEach( async query => {
//   //   await client.query(query)
//   // })
// }

// let serverStatus = [arangoClient, mongoClient];

// const timeQuery = (query, databaseType) => {
//   console.time("Query Timer");

//   return console.timeEnd("QUery Timer");
// };

// console.log("MongoDB vs ArangoDB: The grand showdown!");
// // console.log(serverStatus)

// // console.time("Arango Load Time")
// // arangoLoadNodes(arangoClient, '/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/fake_users.csv' )
// //   .then( ()=> {
// //     console.log('Graph Data Loaded successfully')
// //   })
// //   .catch( (err) => [
// //     console.log('Error Loading Graph Data: ', err )
// //   ])
// arangoLoadNodes(
//   arangoClient,
//   "/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/fake_users.csv"
// );
// arangoLoadEdges(
//   arangoClient,
//   "/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/fake_social_media.csv"
// )
//   .then(() => {
//     console.log("Graph Data Loaded successfully");
//   })
//   .catch((err) => [console.log("Error Loading Graph Data: ", err)]);
// // console.timeEnd("Arango Load Time")
// // console.timeLog("Arango Load Time")
