var async = require('async');
var Socket = require('simple-socket').Socket;
var events = require('events');
var util = require('util');
var writers = require('./writers.js');
var streams = require('./streams.js');
var utils = require('./utils.js');
var types = require('./types.js');

// debug simple socket
require('simple-socket').debug(console.log);

var optionsDefault = {
  port:  9042,
  version: '3.0.0',
  //max simultaneous requests (before waiting for a response) (max=128)
  maxRequests: 32,
  //When the simultaneous requests has been reached, it determines the amount of milliseconds before retrying to get an available streamId
  maxRequestsRetry: 100,
  //Connection timeout: idle time before attempting to reconnect
  connectionTimeout: 1000,
  // Retry interval: the time between connection attemps
  retryInterval: 1000,
  //Default page size, it can be overridden per execution
  pageSize: 5000
};

var ids = 0;

/**
 * A Cassandra Connection Class
 */
function Connection(options) {
  events.EventEmitter.call(this);

  this.streamHandlers = {};
  this.options = utils.extend({}, optionsDefault, options);

  this.id = ids++;
  this.connects = 0;
  this.reconnects = 0;
  this.connected = false;

  // setup socket client
  this.socketClient = new Socket({host: this.options.host, port: this.options.port, timeout: this.options.connectionTimeout});
  this.socketClient.onDisconnect = this.onDisconnect.bind(this);
  this.socketClient.onTimeout = this.close.bind(this);
}
util.inherits(Connection, events.EventEmitter);

/**
 * Returns if writable or not
 */
Connection.prototype.writable = function() { 
  return !this.isShutdown && this.connected && this.socketClient.writable(); 
};

/** 
 * Connects a socket and sends the startup protocol messages, including authentication and the keyspace used. 
 */
Connection.prototype.open = function(callback) {
  var self = this;

  self.emit('log', 'info', 'Connecting to ' + this.options.host + ':' + this.options.port);
  async.series([
    self.socketClient.connect.bind(self.socketClient),
    self.setup.bind(self),
    self.sendStartup.bind(self),
    self.setKeyspace.bind(self)
  ], function(err) {
    // shutdown connection if shutdown was called while we were connecting
    if (this.isShutdown) return self.shutdown(callback);
    // if there is a callback we only try once
    if (callback) return callback(err);
    if (!err) {
      self.connected = true;
      return self.connects++;
    }
    // else attempt to connect again
    self.emit('log', 'error', 'connection error ', err);
    self.emit('log', 'info', 'reconnecting...' + self.reconnects++);
    setTimeout(self.open.bind(self), self.options.retryInterval);
  });
};

/**
 * Close the connection and trigger reconnect
 */
Connection.prototype.close = function(callback) {
  this.emit('log', 'info', 'disconnecting');

  var self = this;
  Object.keys(this.streamHandlers).forEach(function(streamId) {
    var error = new types.DriverError('Socket was closed')
    error.isServerUnhealthy = true;
    self.freeStreamId({streamId: streamId, error: error});
  });

  if (this.writeQueue) {
    this.writeQueue.stop();
  }
  this.connected = false;
  this.socketClient.disconnect(callback);
};

/**
 * Close without reconnect
 */
Connection.prototype.shutdown = function(callback) {
  this.emit('log', 'info', 'shutdown');

  this.socketClient.onDisconnect = null;
  this.socketClient.onTimeout = null;
  this.isShutdown = true;
  this.close(callback);
};

/**
 * Handle connection disconnect
 */
Connection.prototype.onDisconnect = function() {
  this.emit('disconnect', this);
  this.open();
};

/**
 * setup socket callbacks and streams
 */
Connection.prototype.setup = function(callback) {
  // create a new write queue
  this.writeQueue = new writers.WriteQueue(this.socketClient);

  // setup streams
  var protocol = new streams.Protocol({objectMode: true});
  this.parser = new streams.Parser({objectMode: true});
  var resultEmitter = new streams.ResultEmitter({objectMode: true});
  this.socketClient
    .pipe(protocol)
    .pipe(this.parser)
    .pipe(resultEmitter);

  resultEmitter.on('result', this.handleResult.bind(this));
  resultEmitter.on('row', this.handleRow.bind(this));
  resultEmitter.on('frameEnded', this.freeStreamId.bind(this));

  callback();
};

/**
 * Send startup message and authenticate if necessary
 */
Connection.prototype.sendStartup = function(callback) {
  var self = this;
  self.sendStream(new writers.StartupWriter(self.options.version), null, function(err, response) {
    if (response && response.mustAuthenticate) return self.authenticate(callback);
    callback(err);
  });
};

/**
 * Authenticate with the username and password
 */
Connection.prototype.authenticate = function(callback) {
  if (!this.options.username) {
    return callback(new Error("Server needs authentication which was not provided"));
  }
  this.sendStream(new writers.CredentialsWriter(this.options.username, this.options.password), null, callback);
};

/**
 * Set the kespace if it's present
 */
Connection.prototype.setKeyspace = function(callback) {
  if (!this.options.keyspace) return callback();
  this.execute('USE ' + this.options.keyspace + ';', null, callback);
};

/**
 * Executes a query sending a QUERY stream to the host
 */
Connection.prototype.execute = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'executing query: ' + args.query);
  args.options = utils.extend({pageSize: this.options.pageSize}, args.options);
  this.sendStream(
    new writers.QueryWriter(args.query, args.params, args.consistency, args.options.pageSize, args.options.pageState),
    null,
    args.callback
  );
};

/**
 * Executes a (previously) prepared statement and yields the rows into a ReadableStream
 * @returns {ResultStream}
 */
Connection.prototype.stream = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'Executing for streaming prepared query: 0x' + args.query.toString('hex'));

  var resultStream = new types.ResultStream({objectMode:true});
  this.sendStream(
    new writers.ExecuteWriter(args.query, args.params, args.consistency),
    utils.extend({}, args.options, {resultStream: resultStream}),
    args.callback);
  return resultStream;
};

/**
 * Executes a (previously) prepared statement with a given id
 * @param {Buffer} queryId
 * @param {Array} [params]
 * @param {Number} [consistency]
 * @param [options]
 * @param {function} callback
 */
Connection.prototype.executePrepared = function () {
  var args = utils.parseCommonArgs.apply(null, arguments);
  this.emit('log', 'info', 'Executing prepared query: 0x' + args.query.toString('hex'));
  //When using each row, the final (end) callback is optional
  if (args.options && args.options.byRow && !args.options.rowCallback) {
    args.options.rowCallback = args.callback;
    args.callback = null;
  }
  this.sendStream(
    new writers.ExecuteWriter(args.query, args.params, args.consistency),
    args.options,
    args.callback);
};

/**
 * Prepares a query on a host
 * @param {String} query
 * @param {function} callback
 */
Connection.prototype.prepare = function (query, callback) {
  this.emit('log', 'info', 'Preparing query: ' + query);
  this.sendStream(new writers.PrepareQueryWriter(query), null, callback);
};

/**
 * Sends a Batch request containing multiple queries
 * @param {Array} queries Array of objects with the properties query and params
 * @param {Number} consistency
 * @param {Object} options
 * @param {function} callback
 */
Connection.prototype.executeBatch = function (queries, consistency, options, callback) {
  this.sendStream(new writers.BatchWriter(queries, consistency, options), options, callback);
};

Connection.prototype.register = function register (events, callback) {
  this.sendStream(new writers.RegisterWriter(events), null, callback);
};

/**
 * Uses the frame writer to write into the wire
 * @param frameWriter
 * @param [options]
 * @param {function} [callback]
 */
Connection.prototype.sendStream = function (frameWriter, options, callback) {
  var self = this;
  this.getStreamId(function getStreamIdCallback(streamId) {
    if (!this.writable()) {
      // socket closed while getting stream
      var err = new Error('socket not writable');
      err.isServerUnhealthy = true;
      this.availableStreamIds.push(streamId);
      return callback(err);
    }
    this.emit('log', 'info', 'Sending stream #' + streamId);
    frameWriter.streamId = streamId;

    self.streamHandlers[frameWriter.streamId] = {
      callback: callback,
      frameWriter: frameWriter,
      options: options
    };

    this.writeQueue.push(frameWriter, writeCallback);
  });
  if (!callback) {
    callback = function noop () {};
  }

  function writeCallback(err) {
    if (err) {
      if (!(err instanceof TypeError)) {
        //TypeError is raised when there is a serialization issue
        //If it is not a serialization issue is a socket issue
        err.isServerUnhealthy = true;
      }
      self.freeStreamId({streamId: frameWriter.streamId, error: err});
      if (err.isServerUnhealthy) self.close();
      return;
    }
    if (frameWriter instanceof writers.ExecuteWriter) {
      if (options && options.byRow) {
        self.parser.setOptions(frameWriter.streamId, {byRow: true, streamField: options.streamField});
      }
      else if (options && options.resultStream) {
        self.parser.setOptions(frameWriter.streamId, {resultStream: options.resultStream});
      }
    }
    self.emit('log', 'info', 'Sent stream #' + frameWriter.streamId);
  }
};

Connection.prototype.getStreamId = function(callback) {
  var self = this;
  if (!this.availableStreamIds) {
    this.availableStreamIds = [];
    if (this.options.maxRequests > 128) {
      throw new Error('Max requests can not be greater than 128');
    }
    for(var i = 0; i < this.options.maxRequests; i++) {
      this.availableStreamIds.push(i);
    }
    this.getStreamQueue = new types.QueueWhile(function () {
      return self.availableStreamIds.length === 0;
    }, self.options.maxRequestsRetry);
  }
  this.getStreamQueue.push(function () {
    var streamId = self.availableStreamIds.shift();
    callback.call(self, streamId);
  });
};

Connection.prototype.freeStreamId = function(header) {
  var streamId = header.streamId;
  var handler = this.streamHandlers[streamId];
  delete this.streamHandlers[streamId];
  this.availableStreamIds.push(streamId);
  if(handler && handler.callback) {
    handler.callback(header.error, handler.rowLength);
  }
  this.emit('log', 'info', 'Done receiving frame #' + streamId);
};

/**
 * Handles a result and error response
 */
Connection.prototype.handleResult = function (header, err, result) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.emit('log', 'info', 'event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.emit('log', 'info', 'Received frame #' + streamId);

  var callback = handler.callback;
  //set the callback to null to avoid it being called when freed
  handler.callback = null;
  callback(err, result);
};

/**
 * Handles a row response
 */
Connection.prototype.handleRow = function (header, row, fieldStream, rowLength) {
  var streamId = header.streamId;
  if(streamId < 0) {
    return this.emit('log', 'info', 'Event received', header);
  }
  var handler = this.streamHandlers[streamId];
  if (!handler) {
    return this.emit('log', 'error', 'The server replied with a wrong streamId #' + streamId);
  }
  this.emit('log', 'info', 'Received streaming frame #' + streamId);
  handler.rowLength = rowLength;
  handler.rowIndex = handler.rowIndex || 0;
  var rowCallback = handler.options && handler.options.rowCallback;
  if (rowCallback) {
    rowCallback(handler.rowIndex++, row, fieldStream, rowLength);
  }
};

exports.Connection = Connection;
