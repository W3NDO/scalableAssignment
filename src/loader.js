const { MongoClient, ServerApiVersion } = require("mongodb");
const arangojs = require('arangojs');
const { aql } = require('arangojs/aql')
const fs = require('fs');
const { parse } = require('csv-parse')
const { finished } = require('stream/promises')

/***
 * ARANGO Client Setup
 */

const { Graph, GraphEdgeCollection, GraphVertexCollection } = require('arangojs/graph');

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


const mongoUri = "mongodb://localhost:27888"
const mongoClient = new MongoClient(mongoUri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict:true,
    depreceationErrors: true
  }
});

async function pingMongo(){
  try{
    await mongoClient.connect();
    serverStatus[0] = true
    await mongoClient.db("admin").command({ ping: 1})
    console.log("Pinged scalableMongo")
  } finally {
    await mongoClient.close();
  }
}

pingMongo().catch(console.dir);


/***
 * Other stuff
 */

// LOAD GRAPH

async function loadDataAsObjects(filePath) {
  const records = [];
  const parser = fs
    .createReadStream(filePath)
    .pipe(parse({
    // CSV options if any
    }));
  parser.on('readable', function(){
    let record; while ((record = parser.read()) !== null) {
    // Work with each record
      records.push(record);
    }
  });
  await finished(parser);
  return records;
};

async function arangoLoadGraphData(client, filePath){
  var records = await loadDataAsObjects(filePath)
  records = records.slice(100, 150);
  for(const record of records ){
    console.log((record[1]))
    const [edge, srcNode, destNode] = record
    if (String(edge) === 'description' ){ continue } 
    if (String(edge) === 'rdf-schema#comment'){ continue }
    if(String(edge) === 'concept'){ continue }

    var srcNodeInsertQuery = aql`
      INSERT { 
        srcNodeKey: ${srcNode},
        anime: ${srcNode} 
      }  INTO "anime-ontology" RETURN NEW`

    var destNodeInsertQuery = aql`
      INSERT { 
        destNodeKey: ${srcNode},
        ${edge}: ${String(destNode)}
      } INTO "anime-ontology" RETURN NEW`

    var buildEdgesQuery = aql`` //TODO get the srcNodeKey id, get the destNodekey, get the edge, link

    let arangoSrcNodeID = await client.query(srcNodeInsertQuery);
    let arangoDestNodeID = await client.query(destNodeInsertQuery);

    console.log("SRC = ", arangoSrcNodeID, "DEST = ", arangoDestNodeID)
    // await client.query(
    //   `UPSERT { _from: 'anime-ontology/${arangoSrcNodeID}', _to: 'anime-ontology/${arangoDestNodeID}'} 
    //   INSERT { _from: 'anime-ontology/${arangoSrcNodeID}', _to: 'anime-ontology/${arangoDestNodeID}'} 
    //   UPDATE {} INTO "anime-ontology-edges" `   
    // );
  }  
}



let serverStatus = [arangoClient, mongoClient]

const timeQuery = (query, databaseType) => {
  console.time('Query Timer')

  return console.timeEnd('QUery Timer')
}

console.log("MongoDB vs ArangoDB: The grand showdown!")
// console.log(serverStatus)

// console.time("Arango Load Time")
arangoLoadGraphData(arangoClient, '/home/w3ndo/Desktop/Course Work/Scalable Data Management Systems/project/src/datasets/anime_edge_definition.csv' )
  .then( ()=> {
    console.log('Graph Data Loaded successfully')
  })
  .catch( (err) => [
    console.log('Error Loading Graph Data: ', err )
  ])
// console.timeEnd("Arango Load Time")
// console.timeLog("Arango Load Time")