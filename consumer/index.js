const express = require("express");
const kafka = require("node-rdkafka");
const eventTypes = require("../eventTypes");
const app = express();
const Port = 3001;

const consumer = kafka.KafkaConsumer(
  { "group.id": "kafka", "metadata.broker.list": "localhost:9092" },
  {}
);

consumer.connect();

consumer
  .on("ready", () => {
    console.log("consumer is ready");
    consumer.subscribe(["test"]);
    consumer.consume();
  })
  .on("data", (data) => {
    console.log("msg receive  ", eventTypes.fromBuffer(data.value));
  });

app.get("/", (req, res) => {
  res.send("hello");
  return;
});

app.listen(Port, () => {
  console.log(`consumer server started at port ${Port} `);
});
