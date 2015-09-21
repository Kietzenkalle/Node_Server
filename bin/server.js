var dnode = require('dnode');
var net = require('net');

//setup redis connection on localhost, standard port
var redis = require("redis"),
    client = redis.createClient();
client.on("error", function (err) {
  console.log("Error " + err);
});


var http = require('http');

//for JMeter-Test
/*
var serverx = http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type' : 'text/plain'});
  getDeviceData("Stromking1@ServerA", "Device", "ServerB", function (err, result){
    res.end(result);
  } )
  //res.end("hallo");
});

// Listen to a specified port, or default to 8000
serverx.listen(8000);
*/







//create local Bus, done at every server start
var localPSQueue;
var httpServer = http.createServer(); // create the http server to serve as the federation server. you can also use express if you like...
httpServer.listen(8881);
var Bus = require('busmq'); //options for local bus
var options = {
  redis: 'redis://127.0.0.1', // connect this bus to a local running redis
  //logger: console,
  logLevel: 'debug',
  federate: { // also open a federation server
    server: httpServer,  // use the provided http server as the federation server
    secret: 'mysecret',   // a secret key for authorizing clients
    path: '/my/fed/path'
  }
};
var bus = Bus.create(options);
bus.on('online', function(){
  localPSQueue = bus.queue('localPS');
  localPSQueue.on('message', function(message,id) {
    console.log("Publish am eigenen Server: "+ message);
  })
  localPSQueue.attach();
  localPSQueue.consume();
})
bus.connect();


//set up listening socket for RPCRequests on Port 5004
//call requested function und use callback function with result
var server = net.createServer(function (c) {
  var d = dnode({
    getDeviceData : function (service, device, cb) {
      getLocalData(service, device, function (err, result) {
        cb(result);
      })
    },
    getServices : function(s, cb) {
      getLocalServices(function (err, result) {
        cb(result);
      })
    },
    subscribe : function (service, device, cb) {
      subscribe(service, device, ServerName, function (err, result) {
        cb(result);
      })
    },
    unsubscribe : function (service, device, cb) {
      unsubscribe(service, device, ServerName, function (err, result){
        cb(result);
      })
    }
  });
  c.pipe(d).pipe(c);
});
server.listen(5004);


//slice the console arguments on server start. if first argument is set, it will become the server name, else "ServerA" as default
//with no arguments, serverName has to been set manually before starting transactions
var args= process.argv.slice(2);
var ServerName="ServerA"; //defaultServerName: "ServerA"
if (args.length>0) {
  ServerName = args[0];
}

//publish queues, array of all PublishQueues that get federated
var PublishQueues = {};
//rpcchannel, all rpcqueues to other servers
var RPCChannel ={};




//add new Trusted Cloud
function addTrustedCloud(name, ip){
  client.hset("server:"+name, "ip", ip); //Cloud in Redis speichern

  //create Subscribe-Queue
  var q = bus.queue(name+"."+ServerName);
  q.on('attached', function() {
    console.log('attached to queue. messages will soon start flowing in...');
  });
  q.on('message', function(message, id) {
    console.log('Publish von '+name+': ' + message);
    // Publish msg received
  });
  q.attach();
  q.consume(); // the 'message' event will be fired when a message is retrieved

  //create new bus for federated queue
  var options = {
    logger: console,
    logLevel: 'debug',
    federate: { // also connect to a federate bus
      poolSize: 1, // keep the pool size with 5 web sockets
      urls: ['http://'+ip+':8881/my/fed/path'],  // pre-connect to these urls, 5 web sockets to each url
      secret: 'mysecret'  // the secret ket to authorize with the federation server
    }
  };

  var federatedBus = Bus.create(options);
  federatedBus.on('online', function() {
    //create the queue to be federated
    var fed = federatedBus.federate(bus.queue(ServerName + "." + name), 'http://' + ip + ':8881/my/fed/path');

    fed.on('ready', function (q) {
      // federation is ready - we can start using the queue
      q.on('attached', function () {
        // do whatever
      });
      q.attach();
      PublishQueues[name] = q;
    });
  })
  federatedBus.on('error', function(e){
    console.log("Keine Verbindung zum Zielserver möglich")})

  //setup RPCQueue
  var d = dnode();
  d.on('remote', function (remote) {
    RPCChannel[name]=remote;
  })
  var c = net.connect(5004,ip);
  c.on('error', function(err){
    console.log("Keine Verbindung zur RPCQueue möglich: "+err);
  })
  c.pipe(d).pipe(c);

}


/**
 * add new local Service
 * @param name -name of the new Service
 */
function addService(name){
  client.sadd("services", name);
}


/**
 * get local Services
 * @param callback -called to return results
 */
function getLocalServices(callback){
  client.smembers("services", function(err,obj){
    callback(null, obj+" @"+ServerName);
  });
}


/**
 * get all services, wait 1s for all results
 * @param callback -called to return results
 */
function getServices(callback) {
  var localServices="";
  //get all Services of connected clouds
  for (var key in RPCChannel){
    if(RPCChannel.hasOwnProperty(key)){
      RPCChannel[key].getServices('beep', function (s) {
        localServices=localServices+" -- "+s;
      });
    }
  }
  //add local services to the list
  getLocalServices( function(err,result){
    localServices=localServices+ " -- "+result;
  })
  setTimeout(function() {
    callback(null, localServices);
  }, 1000);
}


/**
 * creates a user and sets his device
 * @param name -name of new user
 * @param device -name of his device
 */
function createUser(name, device){
  client.set("user:"+name, device);
}


/**
 * creates a device
 * @param name -name of the new device
 * @param data -data of the new device
 */
function createDevice(name, data){
    client.hmset("device:"+ name, "Data", data);
}


/**
 * setData on device, check if there are subscribers, if yes, publish the data change
 * @param device -name of the device
 * @param data -data value
 */
function setData(device, data){
  client.hset("device:"+device, "Data", data);
  getSubscribers(device, function(err, result){
    if (result.length>0){
      var help;
      var msg;
      result.forEach(function(entry){
        help = entry.split("@");
        msg = entry+" "+ device+"@"+ServerName+": "+ data;
        if(help[1]==ServerName){
          localPSQueue.push(msg);
        }
        else{
          //publish to target server
          try {
            PublishQueues[help[1]].push(msg);
          }
          catch(err){console.log("Subscriber nicht verbunden");}
        }
      })
    };
  })
}


/**
 * get data from local device
 * @param service -name of the requesting service
 * @param device -name of the device
 * @param callback -called to return result
 */
function getLocalData(service, device, callback){
  var array =[];
  getServiceAccess(device, function(err, obj) {
    array = obj;
    if (array.indexOf(service) > -1) {
      client.hget("device:" + device, "Data", function (err, obj) {
        callback(null, obj);
      })
    }
    else {
      callback(null, "Keine Zugriffsberechtigung");
    }
  })
}


/**
 * get data from any device, checks if the device is on the local server, else requests the data on target server
 * @param service -name of the requesting service
 * @param device -name of the device
 * @param target -target server
 * @param callback -called to return result
 */
function getDeviceData(service, device, target, callback){
  if(target===ServerName){
    getLocalData(service, device, function (err, result){
      callback(null, result);
    })
  }
  else {
    if(typeof RPCChannel[target]!= 'undefined') {
      RPCChannel[target].getDeviceData(service, device, function (s) {
        callback(null, s);
      });
    }
    else callback(null, "Keine Verbindung zum Ziel vorhanden");
  }
}


/**
 * returns every service that has access to the device
 * @param device -name of the device
 * @param callback -called to return result
 */
function getServiceAccess(device, callback){
  client.hget("device:"+device, "Access", function(err,obj){
    var result = [];
    if(obj!==null) {
      result = obj.split(",");
    }
    callback(null, result);
  });
}


/**
 * grant service access to a service
 * @param device -name of the device
 * @param service -name of the service
 * @param callback -called to return result
 */
function setServiceAccess(device, service, callback){
  getServiceAccess(device, function(err,result){
    result.push(service);
    client.hset("device:"+device, "Access", result);
    callback(null, "Ok");
  });
}


/**
 * remove service access
 * @param device -name of the device
 * @param service -name of the service
 */
function removeServiceAccess(device, service){
  getServiceAccess(device, function(err,result){
    var newArray=[];
    result.forEach(function(eservice){
      if(eservice!=service) newArray.push(eservice);
    })
    client.hset("device:"+device, "Access", newArray);
  })
  getSubscribers(device, function(err,result){
    var newArray=[];
    result.forEach(function(eservice){
      if(eservice!=service) newArray.push(eservice);
    })
    client.hset("device:"+device, "Subscribed", newArray);
  })

}


/**
 * get all active subscribers
 * @param device -name of the device
 * @param callback -called to return result
 */
function getSubscribers(device, callback){
  client.hget("device:"+device, "Subscribed", function(err,obj){
    var result=[];
    if(obj!==null) {
      result = obj.split(",");
    }
    callback(null, result);
  });
}


/**
 * set service subscription
 * @param service -name of the service
 * @param device -name of the device
 * @param target -target server
 * @param callback -called to return result
 */
function subscribe(service, device, target, callback){
  if(target===ServerName){
    getServiceAccess(device, function(err, result){
      if(result.indexOf(service)>-1){
        getSubscribers(device, function(err,result2){
          result2.push(service);
          client.hset("device:"+device, "Subscribed", result2);
        })

        callback(null, "OK");

      }
      else {callback(null,"Keine Zugriffsberechtigung")}
    })
  }
  else {
    try {
      RPCChannel[target].subscribe(service, device, function (s) {
        callback(null, s);
      });
    }
    catch(err){callback(null, "Keine Verbindung zum Zielserver vorhanden");}
  }
}


/**
 * unsubscribe
 * @param service -name of the service
 * @param device -device name of the device
 * @param target -target server
 * @param callback -call to return result
 */
function unsubscribe(service, device, target, callback) {
  if (target == ServerName) {
    getSubscribers(device, function (err, result) {
      var newArray = [];
      result.forEach(function (eservice) {
        if (eservice != service) newArray.push(eservice);
      })
      client.hset("device:" + device, "Subscribed", newArray);
      callback(null, "OK");
    })
  }
  else {
    try {
      RPCChannel[target].unsubscribe(service, device, function (s) {
        callback(null, s);
      })
    }
    catch (err) {
      callback(null, "Keine Verbindung zum Zielserver vorhanden");
    }
  }
}


/**
 * WatcherAwareness
 * @param device -name of the device
 * @param callback -call to return result
 */
function discoverFollowers(device, callback){
  getSubscribers(device, function(err, result){
    callback(null, result);
  })
}





/*
//Tests

client.flushdb();

//addTrustedCloud(args[1], args[2]);
addService("Stromking1");
addService("Stromking2");
createDevice("Device", 10);
createUser("Bob", "Device");

//client.hset("device:Device", "Access", ["Stromking1@ServerA"]);

setTimeout(function() {
  setServiceAccess("Device", "Stromking1@ServerA", function (err,result){
    console.log("setAccess"+result);
  })
}, 1000);

setTimeout(function() {

  unsubscribe("Stromking1@ServerA", "Device", "ServerB", function(err, result){
    console.log(result);
  })
  getDeviceData("Stromking1@ServerA", "Device", "ServerA", function (err, result){
    console.log(result);
  } )
}, 2000);



setTimeout(function() {

    getDeviceData("Stromking1@ServerA", "Device", "ServerB", function (err, result) {
      console.log(result);
    })


}, 5000);




/*
 setTimeout(function() {
 setData("Device","56");
 //removeServiceAccess("Device", "Stromking1@ServerA");
 }, 4000);
 setTimeout(function() {
 setData("Device","57");
 }, 5000);
 setTimeout(function() {
 unsubscribe("Stromking1@ServerA", "Device", "ServerB", function(err, result){
 console.log("Unsubscribe SK1@A Device@B: "+ result);
 });
 }, 6000);*/


//client.quit();
