/*jslint node: true */

'use strict';

var Q = require('q');
var se = require('stream-extra');
var net = require('net');
var debug = require('debug')('pm-master');
var _ = require('underscore');

var PMMaster = function(opts) {
  this.port = opts.port || 2492;
  this.ctlPort = opts.ctlPort || 2493;
  this.clients = [];
  this.ctlClients = [];
};

module.exports = PMMaster;

PMMaster.prototype.listen = function() {
  if (this.server) {
    return new Q(new Error('server is not null'));
  }

  var deferred = Q.defer();
  this.server = net.createServer(
    this._onRawSocketConnection.bind(this)
  );
  this.server.listen(this.port, deferred.resolve);
  this.server.once('error', deferred.reject);
  this.lastClientId = 0;
  return deferred.promise;
};

PMMaster.prototype._onRawSocketConnection = function(socket) {
  debug('got raw client socket');
  var mux = new se.Mux(new se.Buffered(socket, {
    lengthEncoding: 4
  }));
  var jsonStream = new se.JSON(mux);
  var client = {
    id: this.lastClientId++,
    socket: socket,
    mux: mux,
    info: {},
  };
  client.rpc = new se.RPC(
    jsonStream,
    this._getClientMethods(client)
  );
  this.clients.push(client);
  this._updateInfo(client);
  socket.on('close', _.partial(
    this._onRawSocketClose.bind(this),
    client
  ));
};

PMMaster.prototype._onRawSocketClose = function(client) {
  debug('client socket close');
  var index = this.clients.indexOf(client);
  if (index > -1) {
    this.clients.splice(index, 1);
  } else {
    debug('client not found');
  }
};

PMMaster.prototype._updateInfo = function(client) {
  return client.rpc.call('getInfo')
  .then(function(info) {
    debug('got client info of ' + info.hostname);
    client.info = info;
  }).fail(function(err) {
    console.error('client getInfo failed: ' + err.message);
  });
};

PMMaster.prototype._getClientMethods = function(client) {
  return {
    loop: this.loop.bind(this),
    onProcessEnd: _.partial(
      this.onProcessEnd.bind(this),
      client
    ),
  };
};

// public methods

PMMaster.prototype.loop = function(data) {
  return new Q(data);
};

PMMaster.prototype.onProcessEnd = function(client, procInfo) {
  debug(
    (client.info.hostname || client.socket.remoteAddress) +
    ':' + procInfo.coreId + ' finished ' + procInfo.name +
    ' with code ' + procInfo.code
  );
  return new Q(true);
};

// ctl code

PMMaster.prototype.listenControll = function() {
  if (this.ctlServer) {
    return new Q(new Error('ctlServer is not null'));
  }

  var deferred = Q.defer();
  this.ctlServer = net.createServer(
    this._onRawCtlSocketConnection.bind(this)
  );
  this.ctlServer.listen(this.ctlPort, deferred.resolve);
  this.ctlServer.once('error', deferred.reject);
  return deferred.promise;
};

PMMaster.prototype._onRawCtlSocketConnection = function(socket) {
  debug('got raw ctl client socket');
  var mux = new se.Mux(new se.Buffered(socket, {
    lengthEncoding: 4
  }));
  var jsonStream = new se.JSON(mux);
  var ctlClient = {
    socket: socket,
    mux: mux,
  };
  ctlClient.rpc = new se.RPC(
    jsonStream,
    this._getCtlMethods(ctlClient)
  );
  this.ctlClients.push(ctlClient);
  socket.on('close', _.partial(
    this._onRawCtlSocketClose.bind(this),
    ctlClient
  ));
};

PMMaster.prototype._onRawCtlSocketClose = function(ctlClient) {
  debug('ctl client socket close');
  var index = this.ctlClients.indexOf(ctlClient);
  if (index > -1) {
    this.ctlClients.splice(index, 1);
  } else {
    debug('ctl client not found');
  }
};

PMMaster.prototype._getCtlMethods = function(ctlClient) {
  return {
    loop: this.loop.bind(this),
    getClients: this.getClients.bind(this),
    getProcesses: this.getProcesses.bind(this),
    spawn: _.partial(this.spawn.bind(this), ctlClient),
    kill: this.kill.bind(this),
    getCores: this.getCores.bind(this),
  };
};

// Start of ctl methods.

var outputEncodeClient = function(client) {
  return {
    address: client.socket.remoteAddress,
    family: client.socket.remoteFamily,
    port: client.socket.remotePort,
    info: client.info,
    id: client.id,
  };
};

PMMaster.prototype._getClientsWithProcesses = function() {
  return Q.all(this.clients.map(function(client) {
    return client.rpc.call('getProcesses')
    .then(function(procs) {
      return {
        client: client,
        processes: procs,
      };
    });
  }));
};

PMMaster.prototype.getClients = function(opts) {
  debug('getClients', opts);
  if (!opts) {
    opts = {};
  }

  if (!opts.processes) {
    // no networ call required, just map internal object
    return new Q(this.clients.map(outputEncodeClient));
  }

  // output encoded client processes request
  return this._getClientsWithProcesses()
  .then(function(rs) {
    return rs.map(function(r) {
      return {
        client: outputEncodeClient(r.client),
        processes: r.procs,
      };
    });
  });
};

PMMaster.prototype._getCores = function() {
  debug('_getCores');
  return this._getClientsWithProcesses()
  .then(function(rs) {
    return _.flatten(rs.map(function(r) {
      var client = r.client;
      var procs = r.processes;
      return client.info.cpus.map(function(cpu, i) {
        var running = _.where(procs, {coreId: i});
        if (!running) {
          running = [];
        }

        //debug('cpu', cpu, running);
        return {
          id: i,
          client: client,
          processes: running,
          speed: cpu.speed,
        };
      });
    }));
  });
};

PMMaster.prototype.getCores = function() {
  debug('getCores');
  return this._getCores()
  .then(function(cores) {
    debug('_getCores returned ' + cores.length + ' cores');
    return cores.map(function(core) {
      return _.extend({}, core, {
        client: outputEncodeClient(core.client),
      });
    });
  });
};

PMMaster.prototype.getProcesses = function() {
  debug('getProcesses');
  return Q.all(this.clients.map(function(client) {
    return client.rpc.call('getProcesses')
    .then(function(procs) {
      return procs.map(function(proc) {
        return _.extend({
          client: outputEncodeClient(client),
        }, proc);
      });
    });
  })).then(function(processStatsByClient) {
    return _.flatten(processStatsByClient);
  });
};

PMMaster.prototype._selectCore = function(opts) {
  var deferred = Q.defer();
  if (opts.clientId !== undefined &&
      opts.coreId !== undefined) {
    debug('_selectCore by ids');
    var client = _.findWhere(this.clients, {
      id: opts.clientId
    });
    if (!client) {
      debug(
        'node with id ' + opts.clientId + ' not connected'
      );
      deferred.reject(new Error(
        'node with id ' + opts.clientId + ' not connected'
      ));
    } else {
      var max = client.info.cpus.length;
      if (opts.coreId >= max) {
        debug(
          'node with id ' + opts.clientId +
          ' has only ' + max + ' cpus'
        );
        deferred.reject(new Error(
          'node with id ' + opts.clientId + ' has only ' +
          max + ' cpus'
        ));
      } else {
        deferred.resolve({
          client: client,
          id: opts.coreId,
        });
      }
    }
  } else {
    debug('_selectCore autoselect');
    this._getCores().then(function(cores) {
      var free = _.filter(cores, function(core) {
        // if a hostname is given
        if (opts.hostname) {
          if (core.client.info.hostname !== opts.hostname) {
            return false;
          }
        }

        // if a clientId is given
        if (opts.clientId) {
          if (core.client.id !== opts.clientId) {
            return false;
          }
        }

        return core.processes.length === 0;
      });

      if (!free.length) {
        deferred.reject(new Error('no free cores found'));
      } else {

        //TODO: check if not last is the fastest core

        var bestCore = _.first(_.sortBy(free, function(core) {
          return core.speed;
        }));

        //debug('bestCore', bestCore);
        deferred.resolve(bestCore);
      }
    });
  }

  return deferred.promise;
};

PMMaster.prototype._getSpawnClient = function(opts) {
  var clients = this.clients;
  var client;

  // if hostname is given select client by hostname
  if (opts.hostname) {
    client = _.find(clients, function(client) {
      return (client.info.hostname === opts.hostname);
    });

    if (client) {
      return new Q(client);
    } else {
      return new Q(new Error(
        'hostname ' + opts.hostname + ' not connected'
      ));
    }
  }

  // if the ctl does not set a client id autoselect a free client
  var idPromise = new Q(opts.id);
  if (!opts.id) {
    debug('autoselecting client for spawn');
    idPromise = this.getLeastUsageClient()
    .then(function(clientInfo) {
      debug('best client: ' + clientInfo.info.hostname);
      return clientInfo.id;
    });
  }

  // now the id is set by opts or getLeastUsageClient
  return idPromise.then(function(id) {
    client = _.find(clients, function(client) {
      return (client.id === id);
    });

    if (!client) {
      throw new Error(
        'client with id ' + id + ' not connected'
      );
    }

    return client;
  });
};

PMMaster.prototype.spawn = function(ctlClient, opts) {
  debug('spawn', opts);
  opts = _.extend({
    stdout: false, // if stream
    stderr: false,
    args: [],
  }, opts);
  if (!opts.command) {
    return new Q(new Error('command missing'));
  }

  return this._selectCore(opts)
  .then(function(core) {
    var client = core.client;
    opts.coreId = core.id; // alter opts for rpc call ;-(
    debug(
      'spawn selected client: ' + client.info.hostname +
      ':' + core.id
    );

    // create a deferred to send the info as progress
    var deferred = Q.defer();

    // the info object for the ctrl client.
    var info = {};

    if (opts.promise === 'execution') {
      debug('execution');

      if (opts.stdout) {
        opts.stdout = client.mux.freeId();
        var clientOutStream =
          client.mux.createStream(opts.stdout);
        info.stdout = ctlClient.mux.freeId();
        var ctlOutStream =
          ctlClient.mux.createStream(info.stdout);
        clientOutStream.pipe(ctlOutStream);
        debug(
          'piping stdout of ' + client.info.hostname +
          ':' + opts.stdout + ' to ctl:' + info.stdout
        );
      }

      if (opts.stderr) {
        opts.stderr = client.mux.freeId();
        var clientErrStream =
          client.mux.createStream(opts.stderr);
        info.stderr = ctlClient.mux.freeId();
        var ctlErrStream =
          ctlClient.mux.createStream(info.stderr);
        clientErrStream.pipe(ctlErrStream);
        debug(
          'piping stdout of ' + client.info.hostname + ':' +
          opts.stderr + ' to ctl:' + info.stderr
        );
      }
    }

    client.rpc.call('spawn', opts)
    .then(function(procInfo) {
      // add the client id to the info object
      deferred.resolve(_.extend({
        clientId: client.id,
      }, procInfo));
    }).progress(function() {
      if (opts.promise !== 'execution') {
        debug(
          'error: got an notification from node but spawn ' +
          ' promise type is ' + opts.promise
        );
      } else {
        debug('sending notification to ctrl client', info);
        deferred.notify(info);
      }
    }).fail(deferred.reject);

    return deferred.promise;
  });
};

PMMaster.prototype.kill = function(opts) {
  debug('kill', opts);
  var deferred = Q.defer();
  if (opts.clientId !== undefined && opts.pid !== undefined) {
    var client = _.findWhere(this.clients, {id: opts.clientId});
    if (!client) {
      debug('node with id ' + opts.clientId + ' not connected');
      deferred.reject(new Error(
        'node with id ' + opts.clientId + ' not connected'
      ));
    } else {
      client.rpc.call('kill', opts)
      .then(deferred.resolve)
      .fail(deferred.reject);
    }
  } else {
    deferred.reject(new Error(
      'kill requires clientId and pid'
    ));
  }

  return deferred.promise;
};
