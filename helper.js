const _ = require('lodash');
const config = require('config');
const AWS = require('aws-sdk');
const createCsvWriter = require('csv-writer').createObjectCsvWriter

AWS.config.update({
  region: "us-east-1",
  accessKeyId: config.AWSACCESSID,
  secretAccessKey: config.AWSSECRET
});

const docClient = new AWS.DynamoDB.DocumentClient();

const writeToCSV = async (records) => {
  const csvWriter = createCsvWriter({
    path: `${__dirname}/../../submissions/aggregate.csv`,
    append: true,
    header: [{
      id: 'id',
      title: 'id'
    }, {
      id: 'legacySubmissionId',
      title: 'legacySubmissionId'
    }, {
      id: 'memberId',
      title: 'memberId'
    }, {
      id: 'url',
      title: 'url'
    }, {
      id: 'challengeId',
      title: 'challengeId'
    }, {
      id: 'createdBy',
      title: 'createdBy'
    }, {
      id: 'updatedBy',
      title: 'updatedBy'
    }, {
      id: 'created',
      title: 'created'
    }, {
      id: 'updated,',
      title: 'updated'
    }]
  })

  logger.debug(`writing row to CSV ${records}`)
  return csvWriter.writeRecords([records]) // returns a promise
}


const scanForData = function(ExclusiveStartKey) {
  let params = {
    TableName: 'Submission',
    Limit: 10000,
    FilterExpression: 'attribute_not_exists( #name0 )',
    ExpressionAttributeNames: {
      '#name0': 'legacySubmissionId'
    },
  };

  if (ExclusiveStartKey) {
    params.ExclusiveStartKey = ExclusiveStartKey;
  }

  return docClient.scan(params).promise();
}

const fetchMissingRecords= async () => {
  const missingData = [];
  let LEK = null;

  function addData(data) {
    data.Items.forEach(function (item) {
      console.log(`${JSON.stringify(item.id)}`);
      missingData.push(item);
    });
  }

  do {
    LEK = await scanForData(LEK)
      .then((data, err) => {
        console.log('scan succeeded.');
        console.log(`LastEvaluatedKey: ${JSON.stringify(data.LastEvaluatedKey)}`);
        addData(data);

        if (data.LastEvaluatedKey) {
          return data.LastEvaluatedKey; //scanForData(data.LastEvaluatedKey);
        }

        return null;
      })
      .catch((err) => {
        console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
      });

    console.log(`LEK: ${JSON.stringify(LEK)}`);
  } while (LEK);

  return missingData;
}

module.exports = {
  fetchMissingRecords, writeToCSV
}