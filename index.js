
module.exports = function(config) {
  const sqlite3 = require("sqlite3");
  const { Worker } = require('worker_threads');
  const fs = require('fs');

  let db = null;
  let worker = null;
  let configs = {};
  let dirtyInstances = [];

  const _init = async function(config) {
     db = new sqlite3.Database('ccda.sqlite');
     console.log('Archive Service started');
  }

  const _spawnCleanerWorker = async function() {
    const fileExists = async path => !!(await fs.promises.stat(path).catch(e => false));

    let workerFile = __dirname + '/node_modules/casa-corrently-data-archive/worker.js';
    if(!await fileExists(workerFile)) workerFile = __dirname + '/./worker.js';
    if(!await fileExists(workerFile)) workerFile = './worker.js';

    if((dirtyInstances.length >0 )&&(worker==null)) {
      let uuid = dirtyInstances.pop();
      let config = configs[uuid];
      worker = new Worker(workerFile,{workerData:config});
      worker.on('message', function(_data) {

      });
      worker.on('error', function(e) {
        console.log('Error in Worker',e);
      });
      worker.on('exit', (code) => {
        console.log('Cleaner finished with Code',code);
      });
    } else return;
  }
  const ccda = this;

  const publish = async function(msg,config,memStorage) {
    return new Promise(async (resolve,reject)=>{
      if(db == null) await _init(config);
      if(typeof configs[msg.uuid] == 'undefined') {
        configs[msg.uuid] = config;
        dirtyInstances.push(msg.uuid);
      }
      db.serialize(function() {
          db.run("CREATE TABLE IF NOT EXISTS 'archive_"+msg.uuid+"' (time INTEGER PRIMARY KEY,last24h_price REAL,last7d_price REAL,last30d_price REAL,last90d_price REAL,last180d_price REAL,last365d_price REAL)");
          let cols = [];
          let values = [];
          cols.push("time");
          values.push(msg.time);

          if(typeof msg.stats.last24h !== 'undefined') {
              cols.push('last24h_price');
              values.push(msg.stats.last24h.energyPrice_kwh);
          }

          if(typeof msg.stats.last7d !== 'undefined') {
              cols.push('last7d_price');
              values.push(msg.stats.last7d.energyPrice_kwh);
          }

          if(typeof msg.stats.last30d !== 'undefined') {
              cols.push('last30d_price');
              values.push(msg.stats.last30d.energyPrice_kwh);
          }

          if(typeof msg.stats.last90d !== 'undefined') {
              cols.push('last90d_price');
              values.push(msg.stats.last90d.energyPrice_kwh);
          }

          if(typeof msg.stats.last180d !== 'undefined') {
              cols.push('last180d_price');
              values.push(msg.stats.last180d.energyPrice_kwh);
          }

          if(typeof msg.stats.last365d !== 'undefined') {
              cols.push('last365d_price');
              values.push(msg.stats.last365d.energyPrice_kwh);
          }

          db.run("INSERT into 'archive_"+msg.uuid+"' ("+cols.concat()+")  VALUES ("+values.concat()+")");


          if(dirtyInstances.length > 0) {
            setTimeout(function() {
              _spawnCleanerWorker();
            },500);
          }

          resolve();

      });
    });
  }
  return {
    statics:async function() {
        if(_init == null) await _init(config);
    },
    history:async function(uuid) {
        return new Promise(async (resolve,reject)=>{
            if(db == null) await _init(config);
            db.serialize(function() {
              let history = [];
              db.all("SELECT * FROM 'archive_"+uuid+"' ORDER BY time desc", function(err, rows) {
                    resolve(rows);
                });
            });
        });
    },
    publish: publish
  }
}
