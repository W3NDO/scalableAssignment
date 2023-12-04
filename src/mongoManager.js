const { MongoClient, ServerApiVersion } = require("mongodb");

const mongoUri = "mongodb://localhost:27888"
const mongoClient = new MongoClient(mongoUri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict:true,
    depreceationErrors: true
  }
});

// async function run(){
//   try{
//     await mongoClient.connect();
//     serverStatus[0] = true
//     await mongoClient.db("admin").command({ ping: 1})
//     console.log("Pinged scalableMongo")
//   } finally {
//     await mongoClient.close();
//   }
// }

// run().catch(console.dir);
