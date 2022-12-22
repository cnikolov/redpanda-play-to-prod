import fs from "fs"
import chalk from "chalk"
import parseArgs from "minimist"
import { Kafka } from "kafkajs"
import avro from "avro-js"
import fetch from "node-fetch";

let args = parseArgs(process.argv.slice(2))
const help = `
  ${chalk.green("numbers-producer.js")}
  ${chalk.bold("USAGE")}

    > node hfp-producer.js --help
    > node hfp-producer.js [-b host:port] [-t topic_name] [-r registry_url]
 
  ${chalk.bold("OPTIONS")}

    -h, --help      Shows this help message

    -b, --brokers   Comma-separated list of the host and port for each broker
                      default: localhost:9092

    -t, --topic     Topic where events are sent
                      default: digitransit-hfp

    -r, --registry  Schema registry URL
                      default: http://localhost:8081
`

if (args.help || args.h) {
  console.log(help)
  process.exit(0)
}

const brokers = (args.b || args.brokers || "localhost:19092").split(",")
const topicName = args.t || args.topic || "numbers-topic"
const schemaRegistry = args.r || args.registry || "http://localhost:18081"

// Connect to Redpanda and create topic
const redpanda = new Kafka({"brokers": brokers})
const producer = redpanda.producer()
await producer.connect()
console.log(chalk.green("Connected to Redpanda at: ") + brokers)

const admin = redpanda.admin()
await admin.connect()
const allTopics = await admin.listTopics()
if (topicName in allTopics) {
  console.log(topicName)
  console.log(chalk.yellow("Deleting topic: ") + topicName);
  await admin.deleteTopics({topics: [topicName]})
}
await admin.createTopics({
  topics: [{ 
    topic: topicName,
    numPartitions: 6,
    replicationFactor: 3
  }]
})
await admin.disconnect()

// Register Avro schema
const subject = "numbers-schema"
const deleteSubject = `${schemaRegistry}/subjects/${subject}`
var deleteSubjectResponse = await fetch(deleteSubject, {
  method: "DELETE"
})
if (deleteSubjectResponse.ok) {
  console.log(await deleteSubjectResponse.text())
}

const schema = fs.readFileSync("vp.avsc", "utf-8")
const type = avro.parse(schema)
const post = `${schemaRegistry}/subjects/${subject}/versions`
console.log(chalk.bgGrey("POST: " + post))
var createSchemaResponse = await fetch(post, {
    method: "POST",
    body: JSON.stringify({
      "schema": schema
    }),
    headers: {"Content-Type": "application/json"}
  }
)
if (!createSchemaResponse.ok) {
  console.error(chalk.red("Error registering schema: ") + await createSchemaResponse.text())
  process.exit(1);
}
var createSchemaResponseJson = await createSchemaResponse.json()
const schemaId = createSchemaResponseJson["id"]
console.log(chalk.green("Registered schema: " + schemaId))

//call sendNUmbers
//self calling function

sendNumbers().then(x=>console.log("Done"));
async function sendNumbers(){
  

const defaultOffset = 0
const magicByte = Buffer.alloc(1)
for (let index = 0; index < 1000; index++) {
  const element = {number: index};
  try {
    const msgBuf = type.toBuffer(element)
    const idBuf = Buffer.alloc(4)
    idBuf.writeInt32BE(schemaId, defaultOffset)
    const payload = Buffer.concat([
      magicByte, idBuf, msgBuf
    ])
    const meta = await producer.send({
      topic: topicName,
      messages: [{ value: payload }],
    })
    console.log(chalk.green("Produced: ") + JSON.stringify(meta))
  } catch (e) {
    console.log(chalk.red("Error producing message: ") + element)
    console.error(e)
    process.exit(1)
  }
}
  
}

/* Disconnect on CTRL+C */
process.on("SIGINT", async () => {
  try {
    console.log(chalk.bgGrey("Disconnecting..."))
    await producer.disconnect()
    process.exit(0)
  } catch (e) {
    console.error(e.toString())
    process.exit(1)
  }
})
