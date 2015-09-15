var dnode = require('dnode');
var net = require('net');

//setup redis connection
var redis = require("redis"),
    client = redis.createClient();
client.on("error", function (err) {
  console.log("Error " + err);
});

var sleep = require('sleep');
var http = require('http');

//for testing
var serverx = http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type' : 'text/plain'});
  getDeviceData("Stromking1@ServerA", "Device", "ServerB", function (err, result){
    res.end(result);
  } )
  //res.end("hallo");
});

// Listen to a specified port, or default to 8000
serverx.listen(8000);








//create local Bus
var localPSQueue;
var httpServer = http.createServer(); // create the http server to serve as the federation server. you can also use express if you like...
httpServer.listen(8881);
var Bus = require('busmq');
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

// if you'd like to select database 3, instead of 0 (default), call
// client.select(3, function() { /* ... */ });




//argumente aufnehmen, ServerNamen setzen
var args= process.argv.slice(2);
var ServerName =args[0];

//publish queues
var PublishQueues = {};
//rpcchannel
var RPCChannel ={};




//add new Trusted Cloud
function addTrustedCloud(name, ip){
  client.hset("server:"+name, "ip", ip); //Cloud in Redis speichern

  //empfangsqueue erstellen
  var q = bus.queue(name+"."+ServerName);
  q.on('attached', function() {
    console.log('attached to queue. messages will soon start flowing in...');
  });
  q.on('message', function(message, id) {
    console.log('Publish von '+name+': ' + message);
    // subscribe nachrichten
  });
  q.attach();
  q.consume(); // the 'message' event will be fired when a message is retrieved

  //Bus für PublishNachrichten erstellen
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
  var fed = federatedBus.federate(bus.queue(ServerName+"."+name), 'http://'+ip+':8881/my/fed/path');

  fed.on('ready', function(q) {
    // federation is ready - we can start using the queue
    q.on('attached', function() {
      // do whatever
    });
    q.attach();
    PublishQueues[name]=q;
  });

  //setup RPCQueue
  var d = dnode();
  d.on('remote', function (remote) {
    RPCChannel[name]=remote;
  })
  var c = net.connect(5004,ip);
  c.pipe(d).pipe(c);
}

//add new local Service
function addService(name){
  client.sadd("services", name);
}
//local Services
function getLocalServices(callback){
  client.smembers("services", function(err,obj){
    callback(null, obj+" @"+ServerName);
  });
}

//get all services
// warten auf Ausgabe 1 Sekunde!
function getServices(callback) {
  var localServices="";
  for (var key in RPCChannel){
    if(RPCChannel.hasOwnProperty(key)){
      RPCChannel[key].getServices('beep', function (s) {
        localServices=localServices+" -- "+s;
      });
    }
  }
  getLocalServices( function(err,result){
    localServices=localServices+ " -- "+result;
  })
  setTimeout(function() {
    callback(null, localServices);
  }, 1000);
}

//create a User+Data für Device 1
function createUser(name, device){
  client.set("user:"+name, device);
}

//create Device
function createDevice(name, data){
  // client.hmset("device:"+ name, "Data", data, "Access", [], "Subscribed", [], redis.print);
  client.hmset("device:"+ name, "Data", data);
}

//setData on User:Device:Data
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
        else{ console.log("server:" +help[1]);
          PublishQueues[help[1]].push(msg);
        }
      })
    };
  })

}
//getData from User:Device:Data
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

//get Data allgemein
function getDeviceData(service, device, target, callback){
  if(target===ServerName){
    getLocalData(service, device, function (err, result){
      callback(null, result);
    })
  }
  else {
    RPCChannel[target].getDeviceData(service, device, function (s) {
      callback(null, s);
    });
  }
}



//getServiceAccess
function getServiceAccess(device, callback){
  client.hget("device:"+device, "Access", function(err,obj){
    var result = [];
    if(obj!==null) {
      result = obj.split(",");
    }
    callback(null, result);
  });
}

//setServiceAccess
function setServiceAccess(device, service, callback){
  getServiceAccess(device, function(err,result){
    result.push(service);
    client.hset("device:"+device, "Access", result);
    callback(null, "Ok");
  });
}

//remove Service Access
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

//getSubscribers
function getSubscribers(device, callback){
  client.hget("device:"+device, "Subscribed", function(err,obj){
    var result=[];
    if(obj!==null) {
      result = obj.split(",");
    }
    callback(null, result);
  });
}


//set Service Subscription
function subscribe(service, device, target, callback){
  if(target===ServerName){
    getServiceAccess(device, function(err, result){
      if(result.indexOf(service)>-1){
        getSubscribers(device, function(err,result2){
          result2.push(service);
          client.hset("device:"+device, "Subscribed", result2);
        })

        callback(null, "OK");
        //createSubscriber(username, device, service);
        //device not defined
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

//unsubscribe
function unsubscribe(service, device, target, callback){
  if (target==ServerName){
    getSubscribers(device, function(err,result){
      var newArray=[];
      result.forEach(function(eservice){
        if(eservice!=service) newArray.push(eservice);
      })
      client.hset("device:"+device, "Subscribed", newArray);
      callback(null, "OK");
    })
  }
  else {
    RPCChannel[target].unsubscribe(service, device, function (s) {
      callback(null,s);
    })
  }
}

//get Watchers/WatcherAwareness
function discoverFollowers(device, callback){
  getSubscribers(device, function(err, result){
    callback(null, result);
  })
}


/*
 //send message
 function sendMessage(username, target, type, message){

 var id = ('xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);}));
 var msg = {user:username, type:target, id: id , message:message};
 JSON.stringify(msg);
 console.log("Message sent: "+msg.toString());
 PublishQueues[target].push(msg);


 }

 function sendRequest(service, device, target, type, message){
 var id = ('xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {var r = Math.random()*16|0,v=c=='x'?r:r&0x3|0x8;return v.toString(16);}));
 var msg = {sender:sender, type:type, id: id, message:message};
 console.log("Message: "+msg["sender"]);
 //JSON.stringify(msg);
 PublishQueues[target].push(msg);
 }*/



//Tests
/*
client.flushdb();

addTrustedCloud(args[1], args[2]);
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

  subscribe("Stromking1@ServerA", "Device", "ServerA", function(err, result){
    console.log(result);
  })
  getDeviceData("Stromking1@ServerA", "Device", "ServerA", function (err, result){
    console.log(result);
  } )
}, 2000);



setTimeout(function() {
  for(var i=0; i<5000; i++)
  {
    getDeviceData("Stromking1@ServerA", "Device", "ServerA", function (err, result) {
      console.log(i+" "+result);
    })
  }

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
