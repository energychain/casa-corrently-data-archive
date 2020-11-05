
module.exports = function(config) {
  const sqlite3 = require("sqlite3");

  let db = null;

  const _init = async function(config) {
     db = new sqlite3.Database('ccda.sqlite');
     console.log('Archive Service started');
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
    publish: async function(msg,alias) {
        if(db == null) await _init(config);
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
        });
    }
  }
}
