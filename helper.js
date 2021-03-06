const _ = require('lodash');
const config = require('config');
const AWS = require('aws-sdk');
const createCsvWriter = require('csv-writer').createObjectCsvWriter

AWS.config.update({
  region: "us-east-1",
  accessKeyId: config.AWSACCESSID,
  secretAccessKey: config.AWSSECRETKEY
});

const docClient = new AWS.DynamoDB.DocumentClient();

const writeToCSV = async (records, fileName) => {
  const csvWriter = createCsvWriter({
    path: fileName,
    header: [{
      id: 'id',
      title: 'id'
    }, {
      id: 'legacySubmissionId',
      title: 'legacySubmissionId'
    }, {
      id: 'challengeId',
      title: 'challengeId'
    }, {
      id: 'memberId',
      title: 'memberId'
    }, {
      id: 'url',
      title: 'url'
    }, {
      id: 'submissionPhaseId',
      title: 'submissionPhaseId'
    }, {
      id: 'fileType',
      title: 'fileType'
    }, {
      id: 'type',
      title: 'type'
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

  console.debug(`writing data to CSV: ${records.length} records`)
  return csvWriter.writeRecords(records) // returns a promise
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

    console.log(`LEK: ${_.get(LEK,'id','')}`);
  } while (LEK);

  console.log('no more records');
  return missingData;
}

module.exports = {
  fetchMissingRecords, writeToCSV
}