// All the necessary imports
import { createServer } from 'http';
import fs from 'fs'
import express from 'express';
import AWS from 'aws-sdk';
import bodyParser from 'body-parser';
import request from 'request';
import { get } from 'lodash';
import { createObjectCsvWriter as  createCsvWriter} from 'csv-writer';

// Loading credentials from the system
const credentials = new AWS.SharedIniFileCredentials({profile: 'default'});
const s3 = new AWS.S3({credentials: credentials, region: 'us-west-2'});
AWS.config.update({region:'us-west-2'});
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const destinationBucket = 'aws-managed-services-destination-bucket-pgcc';

// Setting up express app
const app = express();
app.use(bodyParser.json());
app.use(bodyParser.text({ type: 'text/plain' }));

const handleSubscriptionResponse = (error, response) => {
    if (!error && response.statusCode == 200) {
        console.log('Yess! We have accepted the confirmation from AWS');
    }
    else {
        //throw new Error(`Unable to subscribe to given URL`);
        console.log(error);
        //console.error(error)
    }
}

app.post("/publishS3MetaData", async (req, response) => {
    try {
        let payloadStr = req.body;
        const payload = JSON.parse(payloadStr);
        if (req.header('x-amz-sns-message-type') === 'SubscriptionConfirmation') {
            const url = payload.SubscribeURL;
            console.log(url);
            await request(url, handleSubscriptionResponse);
        } else if (req.header('x-amz-sns-message-type') === 'Notification') {
            const message = JSON.parse(get(payload, 'Message', {}));
            const bucketName =  get(message, 'Records[0].s3.bucket.name', '');
            const fileName  = get(message, 'Records[0].s3.object.key', '');
            const params = {Bucket: bucketName, Key: fileName};
            s3.getObject(params, (err, json_data) => {
                if (!err) {
                    let invoiceInformation = [...json_data.Body.toString('utf-8').split("\n")];
                    invoiceInformation.shift();
                    invoiceInformation.shift();
                    invoiceInformation.shift();
                    invoiceInformation = invoiceInformation.filter((item) => item !== '');
                    const itemsIndex = invoiceInformation.indexOf('Items');
                    let items = [];
                    if (itemsIndex !== -1 && invoiceInformation[itemsIndex + 1]) {
                        items = [...invoiceInformation.slice(itemsIndex + 1)];
                        invoiceInformation = [...invoiceInformation.slice(0, itemsIndex)];
                    }
                    const invoiceData = [];
                    const invoiceReceipt = {};
                    invoiceInformation.forEach((invoice, index) => {
                        const invoiceFragments = invoice.split(":");
                        switch(index) {
                            case 0:
                                invoiceReceipt.customerID = invoiceFragments[1].trim();
                                break;
                            case 1:
                                invoiceReceipt.invoiceID = invoiceFragments[1].trim();
                                break;
                            case 2:
                                invoiceReceipt.date = invoiceFragments[1].trim();
                                break;
                            case 3:
                                invoiceReceipt.from = invoiceFragments[1].trim();
                                break;    
                            case 4:
                                invoiceReceipt.to = invoiceFragments[1].trim()
                                break; 
                            case 5:
                                invoiceReceipt.amount = invoiceFragments[1].trim();
                                break;
                            case 6:
                                invoiceReceipt.sgst = invoiceFragments[1].trim();
                                break; 
                            case 7:
                                invoiceReceipt.total = invoiceFragments[1].trim();
                                break;   
                            case 8:
                                invoiceReceipt.inWords = invoiceFragments[1].trim();
                                break;            
                        }
                    });
                    if (items.length) {
                        invoiceReceipt.items = items;
                    }
                    invoiceData.push(invoiceReceipt);
                    var params = {
                        TableName : 'invoice',
                        Item: invoiceReceipt
                      };
                      
                      var documentClient = new AWS.DynamoDB.DocumentClient();
                      
                      documentClient.put(params, function(err, data) {
                        if (err) console.log(err);
                        else {
                            const queryParams = {
                                TableName: 'invoice',
                                ScanIndexForward: true                             
                            };
                            documentClient.scan(queryParams, function(err, data) {
                                if (err) console.log(err);
                                else {
                                    const csvWriter = createCsvWriter({
                                        path: 'invoices.csv',
                                        header: [
                                          {id: 'customerID', title: 'Customer-ID'},
                                          {id: 'invoiceID', title: 'Invoice-ID'},
                                          {id: 'date', title: 'Date'},
                                          {id: 'from', title: 'From'},
                                          {id: 'to', title: 'To'},
                                          {id: 'sgst', title: 'SGST'},
                                          {id: 'total', title: 'Total'},
                                          {id: 'inWords', title: 'In Words'},
                                          {id: 'items', title: 'Items'},
                                        ]
                                      });
                                    csvWriter
                                    .writeRecords(data.Items)
                                    .then(()=> {
                                        console.log('The CSV file was written successfully');
                                        const fileContent = fs.readFileSync('invoices.csv');
                                        const params = {
                                            Bucket: 'aws-managed-services-destination-bucket-pgcc',
                                            Key: 'invoices.csv', // File name you want to save as in S3
                                            Body: fileContent
                                        };
                                    
                                        // Uploading files to the bucket
                                        s3.upload(params, function(err, data) {
                                            if (err) {
                                                throw err;
                                            }
                                            console.log(`File uploaded successfully. ${data.Location}`);
                                        });

                                    });
                                }
                             });
                        }
                      }); 
                }
            });        
            //process data here
        } else {
            throw new Error(`Invalid message type ${payload.Type}`);
        }
    } catch (err) {
        console.error(err);
        response.status(500).send('Oops');
    }
    response.send('Ok');
});

createServer(app).listen(3000, "0.0.0.0", () => (
    console.log('Started!')
));

module.exports = app;