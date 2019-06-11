const { fetchMissingRecords, writeToCSV } = require('./helper.js');

const missingData = fetchMissingRecords();