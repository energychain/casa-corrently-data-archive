
module.exports = function(config) {
  const sqlite3 = require("sqlite3");
  const { Worker } = require('worker_threads');
  const fs = require('fs');

  let db = null;
  let worker2 = false;
  let configs = {};
  let dirtyInstances = [];
  let memHistory = {}

  const _init = async function(config) {
     db = new sqlite3.Database('ccda_now.sqlite');
     console.log('Archive Service started');
  }

  const _spawnCleanerWorker = async function() {
    const fileExists = async path => !!(await fs.promises.stat(path).catch(e => false));

    let workerFile = __dirname + '/node_modules/casa-corrently-data-archive/worker.js';
    if(!await fileExists(workerFile)) workerFile = __dirname + '/./worker.js';
    if(!await fileExists(workerFile)) workerFile = './worker.js';

    if((dirtyInstances.length >0 )&&(!worker2)) {
      worker2 = true;
      const uuid = dirtyInstances.pop();
      let config = configs[uuid];
      if(typeof memHistory[uuid] == 'undefined') {
            console.log('data-archive:starting',uuid);
            const worker = new Worker(workerFile,{workerData:config});
            worker.on('message', function(_data) {
              if(typeof _data.history !== 'undefined') {
                console.log('data-archive:Received Worker History',_data.uuid,_data.history.length);
                memHistory[_data.uuid] = _data.history;
              }
            });
            worker.on('error', function(e) {
              console.log('data-archive:Error in Worker',e);
              worker2 = false;
              // _spawnCleanerWorker();
            });
            worker.on('exit', (code) => {
              console.log('data-archive:Cleaner finished with Code',code);
              worker2 = false;
              // _spawnCleanerWorker();
            });
      } else {
        worker2 = false;
      }
    } else return;
  }
  const ccda = this;

  const publish = async function(msg,config,memStorage) {
    return new Promise(async (resolve,reject)=>{
      if(db == null) await _init(config);
      if((typeof msg.uuid !== 'undefined') && (msg.uuid !== null) && (typeof configs[msg.uuid] == 'undefined')) {
        configs[msg.uuid] = config;
        dirtyInstances.push(msg.uuid);
        setTimeout(function() {
          _spawnCleanerWorker();
        },5000);
        resolve();
      } else {
        resolve();
      }

    });
  }
  return {
    statics:async function() {
        if(_init == null) await _init(config);
    },
    history:async function(uuid) {
        return memHistory[uuid];
    },
    publish: publish
  }
}
