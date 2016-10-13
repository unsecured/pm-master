/*jslint node: true */

'use strict';

var minimist = require('minimist');

var argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    q: 'quiet',
    v: 'verbose',
    c: 'ctl-port',
    p: 'port',
  },
  boolean: [
    'help',
    'quiet',
    'version',
    'verbose',
  ],
  default: {
    port: 2492,
  },
});

if (argv.version) {
  console.log(require('../package.json').version);
  process.exit(0);
}

if (argv.help) {
  console.log(function () {
  /*
  pm-master - Cluster process manager

  Usage:
    pm-master [OPTIONS]

  Options:
    -p, --port [NUMBER]        port for client connections [defualt: 2492]
    -c, --ctl-port [NUMBER]    start ctl server on this port
    -q, --quiet                only show error output
        --version              print the current version
    -v, --verbose              be verbose

  Please report bugs!  https://github.com/unsecured/pm-master/issues

  */
  }.toString().split(/\n/).slice(2, -2).join('\n'));
  process.exit(0);
}

var qlog = function(msg) {
  if (!argv.quiet) {
    console.log(msg);
  }
};

var PMMaster = require('..');
var master = new PMMaster({
  port: parseInt(argv.port),
  ctlPort: argv['ctl-port'],
});

master.listen().then(function() {
  qlog('server is listening on ' + argv.port);
}).fail(function(err) {
  qlog('failed listening: ' + err.message);
}).then(function() {
  if (argv['ctl-port']) {
    return master.listenControll().then(function() {
      qlog('ctl server is listening on ' + argv['ctl-port']);
    }).fail(function(err) {
      qlog('failed listening on ctl: ' + err.message);
    });
  }
});
