var arangojs = require('arangojs');

const { Graph, GraphEdgeCollection, GraphVertexCollection } = require('arangojs/graph');
// var directed_relation = Graph.("lives_in", "user", "city");
// var undirected_relation = Graph._relation("knows", "user", "user");
// var edgedefinitions = Graph._edgeDefinitions(directed_relation);
// Graph._extendEdgeDefinitions(edgedefinitions, undirected_relation);
// console.log(edgedefinitions);

const arangoClient = arangojs({
    url: "http://localhost:8529",
    databaseName: "TestArango",
    auth: { username: "root", password: "pswd1" },
  });

// db.listCollections()
// .then(function (collections) {
// console.log("Your collections: " + collections.map(function (collection) {
//     return collection.name;
// }).join(", "));
// });


// // const collection = db.collection("anime-ontology");
// const graph = db.graph("testEdgeCollection");
// console.log(graph)
// // const collection = graph.edgeCollection("testEdges")
// // try {
// //   const edge = collection.edge("abc123");
// //   console.log(edge);
// // } catch (e) {
// //   console.error("Could not find edge");
// // }
