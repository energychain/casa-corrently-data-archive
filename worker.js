(async () => {
  const { workerData, parentPort } = require('worker_threads');
  const config = workerData;
  parentPort.postMessage({ 'launching': config.uuid });
  let db = null;
  try {

  const sqlite3 = require("sqlite3");
  const CasaCorrently = require(process.cwd()+"/node_modules/casa-corrently/app.js");
  const main = await CasaCorrently();

  const _init = async function() {
    return new Promise(async function(resolve,rejext) {
     db = new sqlite3.Database('ccda.sqlite');
     db.serialize(function() {
       db.run("CREATE TABLE IF NOT EXISTS 'archive_"+config.uuid+"' (time INTEGER PRIMARY KEY,last24h_price REAL,last7d_price REAL,last30d_price REAL,last90d_price REAL,last180d_price REAL,last365d_price REAL)");
       console.log('Archive Worker started',config.uuid);
       resolve();
     });
   });
  }

  const parentPost = function() {
    return new Promise(async function(resolve,rejext) {
      db.all("SELECT * FROM 'archive_"+ config.uuid+"' ORDER BY time desc", function(err, rows) {
            parentPort.postMessage({ 'uuid': config.uuid, 'history':rows });
            resolve();
      });
    });
  }

  const _retentionRun = function(ts,retention) {
    return new Promise(async function(resolve,rejext) {

        db.each("SELECT COUNT(time) as cnt, avg(time) as time,avg(last24h_price) as last24h_price, avg(last7d_price) as last7d_price, avg(last30d_price) as last30d_price, avg(last90d_price) as last90d_price, avg(last180d_price) as last180d_price, avg(last365d_price) as last365d_price FROM 'archive_"+config.uuid+"' where time<="+ts+" and TIME>"+(ts-retention),
          async function(err, row) {
            if(err) {
              console.log('_retentionRun',err);
            }
            if((row!==null) && (row.cnt)) {
                  db.serialize(function() {
                  db.run("DELETE FROM 'archive_"+config.uuid+"' where time<="+ts+" and TIME>"+(ts-retention));
                  let cols = [];
                  let values = [];
                  cols.push("time");
                  values.push(Math.round(row.time));

                  cols.push('last24h_price');
                  values.push(row.last24h_price);

                  cols.push('last7d_price');
                  values.push(row.last7d_price);

                  cols.push('last30d_price');
                  values.push(row.last30d_price);

                  cols.push('last90d_price');
                  values.push(row.last90d_price);

                  cols.push('last180d_price');
                  values.push(row.last180d_price);

                  cols.push('last365d_price');
                  values.push(row.last365d_price);
                  try {
                  db.run("INSERT into 'archive_"+config.uuid+"' ("+cols.concat()+")  VALUES ("+values.concat()+")");
                } catch(e) {

                }
                  resolve();
                });
            } else {
              const memStorage2 = {
                memstorage:{},
                get:function(key) {
                  return this.memstorage[key];
                },
                set:function(key,value) {
                  this.memstorage[key] = value;
                }
              };
              let msg2 = {
                payload: {},
                topic: 'statistics'
              };
              let msg = null;
              try {
               let time = new Date().getTime() - Math.round(ts - (retention/2));
               msg = await main.meterLib(msg2,config,memStorage2,null,time);
             } catch(e) {

             }
              // Hier Notfallabschaltung via ts=0 einleiten wenn benötigt
              let cols = [];
              let values = [];

              cols.push("time");
              values.push(msg.time);
              if(((msg == null) || (typeof msg.stats == 'undefined')) && (typeof msg.time !== 'undefined')) {
                ts = 0;
                resolve();
              } else {
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
                db.serialize(function() {
                  try {
                  db.run("INSERT into 'archive_"+msg.uuid+"' ("+cols.concat()+")  VALUES ("+values.concat()+")");
                } catch(e) {

                }
                  setTimeout(function() {
                      resolve();
                  },200);
                });
              }
            }
          }
        );
      });
  }

  console.log('Cleaner start ',config.uuid);
  await _init();
  // Mit gegebener Config (eines Zählers) können wir hier ungestört arbeiten und müssen nur am Ende einen Exit machen.
  let ts = new Date().getTime();
  console.log('Clean 24h',config.uuid);
  for(let i=0;(i<96)&&(ts > 0);i++) {
    let retention = 900000;
    ts -= retention;
    await _retentionRun(ts,retention);
    await parentPost();
  }

 console.log('Cleaner 24h finished ',config.uuid);
  for(let i=0;(i<30)&&(ts > 0);i++) {
    let retention = 3600000;
    ts -= retention;
    await _retentionRun(ts,retention);
    await parentPost();
  }
  console.log('Cleaner 30d finished ',config.uuid);
  for(let i=0;(i<365)&&(ts > 0);i++) {
    let retention = 86400000;
    ts -= retention;
    await _retentionRun(ts,retention);
    await parentPost();
  }
  console.log('Cleaner 365d finished ',config.uuid);


  console.log('Cleaner finished ',config.uuid);
  db.close();
  return;
  } catch(e) {
    console.log(e);
    db.close();
    parentPort.postMessage({ 'captured': e });
  }
})();
