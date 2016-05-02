(function() {
  'use strict';

  var cassandra = require('cassandra-driver');
  var async = require('async');
  var util = require('util');
  var _ = require("underscore");
  var model = require('shady-model');
  
  var Database = class {
    
    constructor(options) {
      this._client = new cassandra.Client({contactPoints: options.cassandraContactPoints, keyspace: options.cassandraKeyspace });
      this.Categories = new Categories(this._client);
      this.Places = new Places(this._client);
    }
    
  };
  
  var Categories = class {
    
    constructor(client) {
      this._client = client;
    }
    
    create (id, name, icon, callback) {
      var query = util.format("insert into Category (id, name, icon) values (?, ?, ?)");
      this._client.execute(query, [id, name, icon], callback);
    }
    
    findRow (id, callback) {
      this._client.execute("select * from Category WHERE id=?", [id], function (err, result) {
        if (err) {
          callback(err);
        } else {
          callback(null, result.rows.length > 0 ? result.rows[0] : null);
        }
      });
    }
    
    listRows (ids, callback) {
      if (ids && ids.length) {
        this._client.execute("select * from Category WHERE id in ?", [ids], function (err, result) {
          if (err) {
            callback(err);
          } else {
            callback(null, result.rows);
          }
        });
      }
    }
    
    update (id, name, icon, callback) {
      var query = util.format("update Category set name = ?, icon = ? where id = ?");
      this._client.execute(query, [name, icon, id], callback);
    }
    
    load (ids, callback) {
      if (ids && ids.length) {
        this.listRows(ids, function (err, rows) {
          if (err) {
            callback(err);
          } else {
            callback(null, _.map(rows, function (row) {
              return new model.Category(row['id'], row['name'], row['icon']);
            }));
          }
        });
      } else {
        callback(null, []);
      }
    }
    
    persist (categories, mainCallback) {
      var operations = _.map(categories||[], function (category) {
        return function (callback) {
          this.findRow(category.id, function (err, row) {
            if (err) {
              callback(err);
            } else {
              if (row) {
                this.update(category.id, category.name, category.icon, callback);
              } else {
                this.create(category.id, category.name, category.icon, callback);
              }
            }
          }.bind(this));
        }.bind(this);
      }.bind(this));
      
      async.parallel(operations, function (err) {
        mainCallback(err, categories);
      });
    }
    
  }
  
  var Places = class {

    constructor(client) {
      this._client = client;
      this._categories = new Categories(this._client);
    }
    
    create (id, name, url, description, categories, tags, locationLatitude, locationLongitude, 
        locationAccurate, locationStreetAddress, locationCrossStreet, locationCity, locationState, 
        locationPostalCode, locationCountry, priceLevel, priceMessage, mainCallback) {
      
      var categoryIds = _.map(categories||[], function (category) {
        return category.id;
      });
      
      var data = {
        id: id,
        name: name||'',
        description: description||'',
        url: url||'',
        categories: categoryIds,
        tags: tags||[],
        locationLatitude: locationLatitude||null,
        locationLongitude: locationLongitude||null,
        locationAccurate: locationAccurate||false,
        locationStreetAddress: locationStreetAddress||'',
        locationCrossStreet: locationCrossStreet||'',
        locationCity: locationCity||'',
        locationState: locationState||'',
        locationPostalCode: locationPostalCode||'',
        locationCountry: locationCountry||'',
        priceLevel: priceLevel||null,
        priceMessage: priceMessage||''
      };
      
      var columns = _.keys(data);
      var query = util.format('insert into Place (%s) values (%s)', columns, _.times(columns.length, function () { return '?'; }).join(','));

      this._client.execute(query, _.values(data), mainCallback);
    }
    
    findRow (id, callback) {
      this._client.execute(util.format("select * from Place WHERE id = ?"), [id], function (err, result) {
        if (err) {
          callback(err);
        } else {
          callback(null, result.rows.length > 0 ? result.rows[0] : null);
        }
      });
    }
    
    listRows (ids, callback) {
      if (ids && ids.length) {
        this._client.execute(util.format("select * from Place WHERE id in ?"), [ ids ], function (err, result) {
          if (err) {
            callback(err);
          } else {
            callback(null, result.rows);
          }
        });
      } else {
        callback(null, []);
      }
    }
    
    listAllIds (callback) {
      this._client.execute("select id from Place", [], { fetchSize: 2147483647 }, function (err, result) {
        if (err) {
          callback(err);
        } else {
          callback(null, _.pluck(result.rows, "id"));
        }
      });
    }
    
    update (id, name, url, description, categories, tags, locationLatitude, locationLongitude, 
        locationAccurate, locationStreetAddress, locationCrossStreet, locationCity, locationState, 
        locationPostalCode, locationCountry, priceLevel, priceMessage, mainCallback) {
      
      var categoryIds = _.map(categories||[], function (category) {
        return category.id;
      });
      
      var data = {
        name: name||'',
        description: description||'',
        url: url||'',
        categories: categoryIds,
        tags: tags||[],
        locationLatitude: locationLatitude||'',
        locationLongitude: locationLongitude||'',
        locationAccurate: locationAccurate||false,
        locationStreetAddress: locationStreetAddress||'',
        locationCrossStreet: locationCrossStreet||'',
        locationCity: locationCity||'',
        locationState: locationState||'',
        locationPostalCode: locationPostalCode||'',
        locationCountry: locationCountry||'',
        priceLevel: priceLevel,
        priceMessage: priceMessage||''
      };
      
      var columns = _.map(_.keys(data), function (key) {
        return key + ' = ?';
      });
      
      var query = util.format('update Place set %s where id = ?', columns.join(','));
      var params = _.values(data).concat([id]);
       
      this._client.execute(query, params, function (err, result) {
        mainCallback(err, result);
      });
    }
    
    load (ids, mainCallback) {
      if (ids && ids.length) {
        this.listRows(ids, function (err, rows) {
          if (err) {
            mainCallback(err);
          } else {
            var categoryIds = _.uniq(_.compact(_.flatten(_.pluck(rows, 'categories'))));
            this._categories.load(categoryIds, function (catErr, categories) {
              if (catErr) {
                mainCallback(catErr);
              } else {
                var categoryMap = _.indexBy(categories, 'id');
                
                mainCallback(null, _.map(rows, function (row) {
                  var categories = _.map(row['categories']||[], function (categoryId) {
                    return categoryMap[categoryId];
                  });
  
                  var location = new model.Location(row['locationlatitude'], row['locationlongitude'], row['locationaccurate'], row['locationstreetaddress'], 
                    row['locationcrossstreet'], row['locationcity'], row['locationstate'], row['locationpostalcode'], row['locationcountry']);
                  
                  return new model.Place(row['id'], row['name'], row['description'], row['tags'], categories, row['url'], location, row['pricelevel'], row['pricemessage']);
                }));
              }
            });
          }
        }.bind(this));
      } else {
        mainCallback(null, []);
      }
    }
    
    persist (places, mainCallback) {
      var categories = _.flatten(_.map(places, function (place) {
        return place.categories;
      }), true);
      
      this._categories.persist(categories, function (err) {
        if (err) {
          mainCallback(err);
        } else {
          var operations = _.map(places||[], function (place) {
            return function (callback) {
              this.findRow(place.id, function (err, row) {
                if (err) {
                  callback(err);
                } else {
                  if (row) {
                    this.update(place.id, place.name, place.url, place.description, place.categories, place.tags, 
                        place.location.latitude, place.location.longitude, place.location.accurate, 
                        place.location.streetAddress, place.location.crossStreet, place.location.city, 
                        place.location.state, place.location.postalCode, place.location.country, 
                        place.location.priceLevel, place.location.priceMessage, callback);
                  } else {
                    this.create(place.id, place.name, place.url, place.description, place.categories, place.tags, 
                        place.location.latitude, place.location.longitude, place.location.accurate, 
                        place.location.streetAddress, place.location.crossStreet, place.location.city, 
                        place.location.state, place.location.postalCode, place.location.country, 
                        place.location.priceLevel, place.location.priceMessage, callback);
                  }
                }
              }.bind(this));
            }.bind(this);
          }.bind(this));
          
          async.parallel(operations, function (err, results) {
            mainCallback(err, places);
          });
        }
      }.bind(this));
    }
  }

  module.exports = Database;
  
}).call(this);