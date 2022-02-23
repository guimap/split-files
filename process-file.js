import AWS from 'aws-sdk'
import CSVSplitStream from 'csv-split-stream';
import crypto from 'crypto'
import stream, { Stream } from 'stream';
import { fstat } from 'fs';


AWS.config.update({region: 'us-east-1'});
function start() {
  const sqs = new AWS.SQS({apiVersion: '2012-11-05'});
  const s3 = new AWS.S3({ apiVersion: '2006-03-01'});
  
  const queueURL = 'https://sqs.us-east-1.amazonaws.com/104538124807/handle-file'
  const params = {
    AttributeNames: [
       "SentTimestamp"
    ],
    MaxNumberOfMessages: 10,
    MessageAttributeNames: [
       "All"
    ],
    QueueUrl: queueURL,
    VisibilityTimeout: 20,
    WaitTimeSeconds: 0
   };

   sqs.receiveMessage(params, async function(err, data) {
    if (err) {
      console.log("Receive Error", err);
    } else if (data.Messages) {
      console.time('process')
      await handleMessage(data.Messages[0], s3)
      console.timeEnd('process')
      var deleteParams = {
        QueueUrl: queueURL,
        ReceiptHandle: data.Messages[0].ReceiptHandle
      };
      sqs.deleteMessage(deleteParams, function(err, data) {
        if (err) {
          // console.log("Delete Error", err);
        } else {
          // console.log("Message Deleted", data);
        }
      });
    }
  });
}

async function handleMessage(message, s3Sdk) {
  return new Promise((resolve, reject) => {
    
    const body = JSON.parse(message.Body)
    if (body.Event !== 's3:TestEvent') {
      const [records] = body.Records
      const {s3: { object }} = records
      const bucketParams = { Bucket: 'big-file-bucket', Key: object.key };
      CSVSplitStream.split(
        s3Sdk.getObject(bucketParams).createReadStream(),
        { lineLimit: 10000 },
        () => uploadS3Stream({
          Bucket: 'processed-files-bucket',
          Key: `${crypto.randomUUID()}_${new Date().toISOString()}.csv`
        }, s3Sdk)
      ).then(resolve)
      .catch(reject)
    } else {
       resolve()
    }
  })
    
}

function uploadS3Stream({ Bucket, Key }, s3) {
  const pass = new stream.PassThrough();
  s3.upload({ Bucket, Key, Body: pass }, (err, data) => {
    // console.log(err, data)
  })
  return pass
  // const writable = Stream.Writable()
  // writable._write = (chunk, encoding, next) => {
  //   s3.upload({ Bucket, Key })
  // }
  // writable.on('data', (...args) => {
  //   console.log(...args)
  // })
  // return writable
}


start()