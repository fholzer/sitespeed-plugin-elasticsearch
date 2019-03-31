'use strict';

const log = require('intel').getLogger('sitespeedio.plugin.elasticsearch');
const dayjs = require('dayjs');
const elasticsearch = require('elasticsearch');

class ElasticsearchSender {
  constructor(options) {
    // we need two records in our buffer for each data point
    this.bulkSize = 2 * options.bulkSize;
    this.buffer = [];
    this.client = new elasticsearch.Client(options);
    this.generateIndexName = this.generateIndexName.bind(this, options.indexPattern || '[sitespeed]-YYYY.MM.DD');
  }

  generateIndexName(indexPattern, timestamp) {
    return dayjs(timestamp).format(indexPattern);
  }

  queueOrSend(data) {
    data.forEach(async point => {
      this.buffer.push({ "index": { "_index": this.generateIndexName(point["@timestamp"]), "_type": "doc" } });
      this.buffer.push(point);

      if (this.buffer.length >= this.bulkSize) {
        await this.flush();
      }
    });
  }

  flush() {
    if (this.buffer.length < 1) {
      return Promise.resolve();
    }

    let body = this.buffer;
    this.buffer = [];

    return new Promise((resolve, reject) => {
      this.client.bulk({ body }, (err, resp) => {
        if(err) {
          log.error("Bulk request failed", err, resp);
          return reject(err);
        }
        resolve()
      });
    });
  }
}

module.exports = ElasticsearchSender;
