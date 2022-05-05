"use strict"
const nodemailer = require("nodemailer");
const Realm = require('realm-web');
var CONFIG = require('./config-prod.json');
var assert = require('assert');

async function sendMail({from, origEmail, toEmail, subject, html}){
  /*
    Accessing application's values:
    var x = context.values.get("value_name");

    Accessing a mongodb service:
    var collection = context.services.get("mongodb-atlas").db("dbname").collection("coll_name");
    var doc = collection.findOne({owner_id: context.user.id});

    To call other named functions:
    var result = context.functions.execute("function_name", arg1, arg2);

    Try running in the console below.
  */
  // create reusable transporter object using the default SMTP transport
  let transporter = nodemailer.createTransport({
    host: CONFIG.emailHost,
    port: 465,
    secure: true, // true for 465, false for other ports
    auth: {
      user: CONFIG.emailUser, // generated ethereal user
      pass: CONFIG.emailPass, // generated ethereal password
    }
  });

  // (async function() {
    // send mail with defined transport object
  try {
    let info = await transporter.sendMail({
      from: from || '"MongoDB Consulting" <ps-bot-noreply@mongodb.com>', // sender address
      replyTo: origEmail,
      to: toEmail, // list of receivers
      cc: origEmail,
      subject: subject, // Subject line
      html: html, // html body
    });
    console.log("Message sent: ", info.messageId);
    return null;
  } catch (err) {
    console.log(err.stack);
    return err;
  }
    
  // })()
  
  return null;
};

async function process_message(dbCollection,msg) {
  console.log(`Processing message ${msg._id}`)
  console.log(msg.msg)
  let res = await sendMail(msg.msg);

  if (res)
    dbCollection.updateOne({"_id" : msg._id},{$set:{error:res}});
  else
    dbCollection.updateOne({"_id" : msg._id},{$set:{ts_sent:new Date()}});
}

async function workLoop(dbCollection) {
  console.log("Workloop start")

  const query = { "processed": false };
  const update = {
    "$set": {
      "processed": true,
      "ts_pickup" : new Date()
    }
  };
  const options = { returnNewDocument: true, sort: {ts:1} };

  while(true) {
    let doc = await dbCollection.findOneAndUpdate(query, update, options);
    if (!doc)
      return null;
    await process_message(dbCollection,doc);
  }
}

async function watcher(dbCollection) {
    console.log("Watcher start")
    for await (let event of dbCollection.watch()) {
        const {clusterTime, operationType, fullDocument} = event;

        if ((operationType === 'insert' || operationType === 'update' || operationType === 'replace')
         && !event.fullDocument.processed) {
            const msg = event.fullDocument;

            let res = await dbCollection.updateOne({"_id" : msg._id, "processed" : false},{"$set":{"processed" : true, "ts_pickup" : new Date()}});
            if (res.modifiedCount > 0)
            {
              await process_message(dbCollection,msg);
            } else {
              console.log(`Watcher: someone else picked up the message ${msg._id}`)
            }
        }
    }
  }

const realmApp = new Realm.App({ id: CONFIG.realmAppId });
const realmApiKey = CONFIG.realmApiKey;

async function loginApiKey(apiKey) {
  // Create an API Key credential
  const credentials = Realm.Credentials.apiKey(apiKey);
  // Authenticate the user
  const user = await realmApp.logIn(credentials);
  // `App.currentUser` updates to match the logged in user
  assert(user.id === realmApp.currentUser.id)
  return user
}

loginApiKey(realmApiKey).then(user => {
    console.log("Successfully logged in to Realm!");

    const dbCollection = user
      .mongoClient('mongodb-atlas')
      .db('mailer')
      .collection('messages');

    let timerId = setTimeout(async function watchForUpdates() {
        timerId && clearTimeout(timerId);
        await workLoop(dbCollection);
        await watcher(dbCollection);
        timerId = setTimeout(watchForUpdates, 5000);
    }, 5000);

  }).catch((error) => {
    console.error("Failed to log into Realm", error);
  });
