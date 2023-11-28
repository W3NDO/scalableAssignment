const { MongoClient, ServerApiVersion } = require("mongodb");

let serverStatus = [false, false]

const timeQuery = (query, databaseType) => {
  console.time('Query Timer')

  return console.timeEnd('QUery Timer')
}