'use strict';

const isEmpty = require('lodash.isempty');
const log = require('intel').getLogger('sitespeedio.plugin.elasticsearch');
const Sender = require('./sender');
const DataGenerator = require('./data-generator');

const defaultConfig = {
  indexPattern: '[sitespeed]-YYYY.MM.DD',
  tags: 'category=default',
  includeQueryParams: false
};

module.exports = {
  open(context, options) {
    this.filterRegistry = context.filterRegistry;

    const opts = options.elasticsearch;
    this.options = options;
    this.sender = new Sender(opts);
    this.timestamp = context.timestamp;
    this.resultUrls = context.resultUrls;
    this.dataGenerator = new DataGenerator(opts.includeQueryParams, options);
    this.annotationType = 'webpagetest.pageSummary';
    this.make = context.messageMaker('elasticsearch').make;
    this.sendAnnotation = true;
    this.alias = {};
  },
  processMessage(message, queue) {
    const filterRegistry = this.filterRegistry;

    if (message.type == 'sitespeedio.render') {
      console.log(message)
      return this.sender.flush().then(() => {
        queue.postMessage(this.make('elasticsearch.finished'));
      });
    }

    // First catch if we are running Browsertime and/or WebPageTest
    if (message.type === 'browsertime.setup') {
      this.annotationType = 'browsertime.pageSummary';
    } else if (message.type === 'browsertime.config') {
      if (message.data.screenshot) {
        this.useScreenshots = message.data.screenshot;
        this.screenshotType = message.data.screenshotType;
      }
    } else if (message.type === 'sitespeedio.setup') {
      // Let other plugins know that the Elasticsearch plugin is alive
      queue.postMessage(this.make('elasticsearch.setup'));
    } else if (message.type === 'grafana.setup') {
      this.sendAnnotation = false;
    }

    if (message.type === 'browsertime.alias') {
      this.alias[message.url] = message.data;
    }

    if (
      !(
        message.type.endsWith('.summary') ||
        message.type.endsWith('.pageSummary')
      )
    )
      return;

    // Let us skip this for a while and concentrate on the real deal
    if (
      message.type.match(
        /(^largestassets|^slowestassets|^aggregateassets|^domains)/
      )
    )
      return;

    // we only sends individual groups to Elasticsearch, not the
    // total of all groups (you can calculate that yourself)
    if (message.group === 'total') {
      return;
    }

    message = filterRegistry.filterMessage(message);
    if (isEmpty(message.data)) return;

    let data = this.dataGenerator.dataFromMessage(
      message,
      this.timestamp,
      this.alias
    );

    if (data.length > 0) {
      this.sender.queueOrSend(data)
      // TODO: send annotation data? (screenshot, link to report)
    } else {
      return Promise.reject(
        new Error(
          'No data to send to elasticsearch for message:\n' +
            JSON.stringify(message, null, 2)
        )
      );
    }
  },
  config: defaultConfig
};
