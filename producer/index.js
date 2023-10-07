const express = require("express");
const kafka = require("node-rdkafka");
const eventTypes = require("../eventTypes");
const app = express();
const Port = 3000;

const stream = kafka.Producer.createWriteStream(
  {
    "metadata.broker.list": "localhost:9092",
  },
  {},
  { topic: "test" }
);

function queueMsg() {
  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const event = { category, noise };
  const result = stream.write(eventTypes.toBuffer(event));
  if (result) {
    console.log("message written successfully to stream", result);
  } else {
    console.log("something went wrong");
  }
}

function getRandomAnimal() {
  const categories = ["CAT", "DOG"];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getRandomNoise(animal) {
  if (animal === "CAT") {
    const noises = ["meow", "purr"];
    return noises[Math.floor(Math.random() * noises.length)];
  } else if (animal === "DOG") {
    const noises = ["bark", "woof"];
    return noises[Math.floor(Math.random() * noises.length)];
  } else {
    return "silence..";
  }
}

setInterval(() => {
  queueMsg();
}, 3000);

app.get("/", (req, res) => {
  res.send("hello");
  return;
});

app.listen(Port, () => {
  console.log(`porducer server started at port ${Port} `);
});
