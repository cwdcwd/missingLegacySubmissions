const _ = require('lodash');
const { fetchMissingRecords, writeToCSV } = require('./helper.js');

async function exec(){
  const missingData = await fetchMissingRecords();
  await writeToCSV(missingData, `${__dirname}/missingLegacySubIds.csv`);
}

exec();