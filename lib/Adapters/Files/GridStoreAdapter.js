'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.GridStoreAdapter = undefined;

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _mongodb = require('mongodb');

var _FilesAdapter2 = require('./FilesAdapter');

var _defaults = require('../../defaults');

var _defaults2 = _interopRequireDefault(_defaults);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; } /**
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                GridStoreAdapter
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                Stores files in Mongo using GridStore
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                Requires the database adapter to be based on mongoclient
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 weak
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                */

var GridStoreAdapter = exports.GridStoreAdapter = function (_FilesAdapter) {
  _inherits(GridStoreAdapter, _FilesAdapter);

  function GridStoreAdapter() {
    var mongoDatabaseURI = arguments.length <= 0 || arguments[0] === undefined ? _defaults2.default.DefaultMongoURI : arguments[0];

    _classCallCheck(this, GridStoreAdapter);

    var _this = _possibleConstructorReturn(this, (GridStoreAdapter.__proto__ || Object.getPrototypeOf(GridStoreAdapter)).call(this));

    _this._databaseURI = mongoDatabaseURI;
    return _this;
  }

  _createClass(GridStoreAdapter, [{
    key: '_connect',
    value: function _connect() {
      if (!this._connectionPromise) {
        this._connectionPromise = _mongodb.MongoClient.connect(this._databaseURI);
      }
      return this._connectionPromise;
    }

    // For a given config object, filename, and data, store a file
    // Returns a promise

  }, {
    key: 'createFile',
    value: function createFile(filename, data) {
      return this._connect().then(function (database) {
        var gridStore = new _mongodb.GridStore(database, filename, 'w');
        return gridStore.open();
      }).then(function (gridStore) {
        return gridStore.write(data);
      }).then(function (gridStore) {
        return gridStore.close();
      });
    }
  }, {
    key: 'deleteFile',
    value: function deleteFile(filename) {
      return this._connect().then(function (database) {
        var gridStore = new _mongodb.GridStore(database, filename, 'r');
        return gridStore.open();
      }).then(function (gridStore) {
        return gridStore.unlink();
      }).then(function (gridStore) {
        return gridStore.close();
      });
    }
  }, {
    key: 'getFileData',
    value: function getFileData(filename) {
      return this._connect().then(function (database) {
        return _mongodb.GridStore.exist(database, filename).then(function () {
          var gridStore = new _mongodb.GridStore(database, filename, 'r');
          return gridStore.open();
        });
      }).then(function (gridStore) {
        return gridStore.read();
      });
    }
  },

    /* STREAM FIX */
    {
    key: 'handleVideoStream',
    value: function handleVideoStream(req, res, filename) {
      return this._connect().then(function (database) {
        return _mongodb.GridStore.exist(database, filename).then(function () {
          var gridStore = new _mongodb.GridStore(database, filename, 'r');
          gridStore.open(function(err, GridFile) {
            if(!GridFile) {
              res.send(404,'Not Found');
              return;
            }
            console.log('filename');
            StreamGridFile(GridFile, req, res);
          });
        });
      });
    }
  },
    /* END STREAM FIX */

  {
    key: 'getFileLocation',
    value: function getFileLocation(config, filename) {
      return config.mount + '/files/' + config.applicationId + '/' + encodeURIComponent(filename);
    }
  }, {
    key: 'getFileStream',
    value: function getFileStream(filename) {
      return this._connect().then(function (database) {
        return _mongodb.GridStore.exist(database, filename).then(function () {
          var gridStore = new _mongodb.GridStore(database, filename, 'r');
          return gridStore.open();
        });
      });
    }
  }]);

  return GridStoreAdapter;
}(_FilesAdapter2.FilesAdapter);

exports.default = GridStoreAdapter;

/* STREAM FIX */
function StreamGridFile(GridFile, req, res) {
  var buffer_size = 1024 * 1024;//1024Kb

  if (req.get('Range') != null) { //was: if(req.headers['range'])
    // Range request, partialle stream the file
    console.log('Range Request');
    var parts = req.get('Range').replace(/bytes=/, "").split("-");
    var partialstart = parts[0];
    var partialend = parts[1];
    var start = partialstart ? parseInt(partialstart, 10) : 0;
    var end = partialend ? parseInt(partialend, 10) : GridFile.length - 1;
    var chunksize = (end - start) + 1;

    if(chunksize == 1){
      start = 0;
      partialend = false;
    }

    if(!partialend){
      if(((GridFile.length-1) - start) < (buffer_size) ){
        end = GridFile.length - 1;
      }else{
        end = start + (buffer_size);
      }
      chunksize = (end - start) + 1;
    }

    if(start == 0 && end == 2){
      chunksize = 1;
    }

    res.writeHead(206, {
      'Cache-Control': 'no-cache',
      'Content-Range': 'bytes ' + start + '-' + end + '/' + GridFile.length,
      'Accept-Ranges': 'bytes',
      'Content-Length': chunksize,
      'Content-Type': 'video/mp4',
    });

    GridFile.seek(start, function () {
      // get GridFile stream

      var stream = GridFile.stream(true);
      var ended = false;
      var bufferIdx = 0;
      var bufferAvail = 0;
      var range = (end - start) + 1;
      var totalbyteswanted = (end - start) + 1;
      var totalbyteswritten = 0;
      // write to response
      stream.on('data', function (buff) {
        bufferAvail += buff.length;
        //Ok check if we have enough to cover our range
        if(bufferAvail < range) {
          //Not enough bytes to satisfy our full range
          if(bufferAvail > 0)
          {
            //Write full buffer
            res.write(buff);
            totalbyteswritten += buff.length;
            range -= buff.length;
            bufferIdx += buff.length;
            bufferAvail -= buff.length;
          }
        }
        else{

          //Enough bytes to satisfy our full range!
          if(bufferAvail > 0) {
            var buffer = buff.slice(0,range);
            res.write(buffer);
            totalbyteswritten += buffer.length;
            bufferIdx += range;
            bufferAvail -= range;
          }
        }

        if(totalbyteswritten >= totalbyteswanted) {
          //  totalbytes = 0;
          GridFile.close();
          res.end();
          this.destroy();
        }
      });
    });

  }else{

    //  res.end(GridFile);
    // stream back whole file
    res.header('Cache-Control', 'no-cache');
    res.header('Connection', 'keep-alive');
    res.header("Accept-Ranges", "bytes");
    res.header('Content-Type', 'video/mp4');
    res.header('Content-Length', GridFile.length);
    var stream = GridFile.stream(true).pipe(res);
  }
}
/* END STREAM FIX */