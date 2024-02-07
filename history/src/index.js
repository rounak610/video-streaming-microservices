const express = require("express");
const mongodb = require("mongodb");
const amqp = require('amqplib');
const bodyParser = require("body-parser");

if (!process.env.DBHOST) {
    throw new Error("Please specify the databse host using environment variable DBHOST.");
}

if (!process.env.DBNAME) {
    throw new Error("Please specify the name of the database using environment variable DBNAME");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

const DBHOST = process.env.DBHOST;
const DBNAME = process.env.DBNAME;
const RABBIT = process.env.RABBIT;

function connectDb() {
    return mongodb.MongoClient.connect(DBHOST) 
        .then(client => {
            return client.db(DBNAME);
        });
}

function connectRabbit() {

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    return amqp.connect(RABBIT) // Connect to the RabbitMQ server.
        .then(messagingConnection => {
            console.log("Connected to RabbitMQ.");

            return messagingConnection.createChannel(); // Create a RabbitMQ messaging channel.
        });
}

//
// Setup event handlers.
//
function setupHandlers(app, db, messageChannel) {
    const videosCollection = db.collection("videos");

    function consumeViewedMessage(msg) { 
        console.log("Received a 'viewed' message");

        const parsedMsg = JSON.parse(msg.content.toString());
        
        return videosCollection.insertOne({ videoPath: parsedMsg.videoPath }) 
            .then(() => {
                console.log("Acknowledging message was handled.");
                
                messageChannel.ack(msg); 
            });
    };

    return messageChannel.assertExchange("viewed", "fanout")
        .then(() => {
            return messageChannel.assertQueue("", { exclusive: true });
        })
        .then(response => {
            const queueName = response.queue;
            console.log(`Created queue ${queueName}, binding it to "viewed" exchange.`);
            return messageChannel.bindQueue(queueName, "viewed", "") 
                .then(() => {
                    return messageChannel.consume(queueName, consumeViewedMessage); 
                });
        });
}

function startHttpServer(db, messageChannel) {
    return new Promise(resolve => {
        const app = express();
        app.use(bodyParser.json());
        setupHandlers(app, db, messageChannel);

        const port = process.env.PORT && parseInt(process.env.PORT) || 3000;
        app.listen(port, () => {
            resolve();
        });
    });
}

function main() {
    return connectDb()                                          // Connect to the database...
        .then(db => {                                           // then...
            return connectRabbit()                              // connect to RabbitMQ...
                .then(messageChannel => {                       // then...
                    return startHttpServer(db, messageChannel); // start the HTTP server.
                });
        });
}

main()
    .then(() => console.log("Microservice online."))
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });