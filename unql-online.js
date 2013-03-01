var burrito, couchAuth, couchURL, couchreq, handle, handlers, makeURL
, matchers, opts, pp, processExpr, readline, request, rl, select
, start, url, util, __slice = [].slice;

readline = require('readline');
request = require('request');
burrito = require('burrito');
util = require('util');
url = require('url');

pp = function(x) {
	return util.inspect(x, false, 2, !(process.platform === 'win32' || process.env.NODE_DISABLE_COLORS));
};

module.exports = function(_couchurl){
	couchAuth = null;
	couchURL = _couchurl || 'http://localhost:5984';
	if (!couchURL.match("://")) {
		couchURL = "http://" + couchURL;
	}

	return {
		query: function(expr, cb) {
			var args, i, j, match, matched, matcher, x, _i, _len;
			matched = false;
			for (i = _i = 0, _len = matchers.length; _i < _len; i = ++_i) {
				matcher = matchers[i];
				if (match = expr.match(matcher)) {
					args = (function() {
						var _j, _len1, _results;
						_results = [];
						for (j = _j = 0, _len1 = match.length; _j < _len1; j = ++_j) {
							x = match[j];
							if (j !== 0) {
								_results.push(x);
							}
						}
						return _results;
					})();
					handlers[i].apply(handlers, __slice.call(args).concat([cb]));
					matched = true;
					break;
				}
			}
			if (!matched) {
				return cb("No such command");
			}
		}
	};
}

/*
# Regex matching stuff
# Each user command is matched in order against the regexes specified with 'handle'
# Regexes are case insensitive by default, and anchored to the start and end of the command
*/
matchers = [];
handlers = [];

handle = function(match, fn) {
	match = new RegExp("^" + match.source + "$", 'i');
	matchers.push(match);
	return handlers.push(fn);
};

/*
# Helper methods for accessing CouchDB
*/

makeURL = function(db, doc) {
	if (doc == null) { doc = ''; }
	db = db.replace(/\/$/, '');
	if (db.match("://")) {
		return "" + db + "/" + doc;
	} else {
		return "" + couchURL + "/" + db + "/" + doc;
	}
};

couchreq = function(method, db, doc, data, cb) {
	var headers, req, _ref;
	if (cb === void 0) {
		_ref = [null, doc, data], doc = _ref[0], data = _ref[1], cb = _ref[2];
	}
	url = makeURL(db, doc);
	headers = couchAuth && !db.match("://") ? { Authorization: couchAuth } : {};
	req = {
		method: method,
		uri: makeURL(db, doc),
		headers: headers,
		json: data
	};
	return request(req, function(err, req, body) {
		if (err) {
			return cb(err);
		} else if (body.error) {
			return cb(body);
		} else {
			return cb(null, body, req);
		}
	});
};

couchreq.get = function() {
  var args;
  args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
  return couchreq.apply(null, ['GET'].concat(__slice.call(args)));
};

couchreq.put = function() {
  var args;
  args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
  return couchreq.apply(null, ['PUT'].concat(__slice.call(args)));
};

couchreq.post = function() {
  var args;
  args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
  return couchreq.apply(null, ['POST'].concat(__slice.call(args)));
};

couchreq["delete"] = function() {
  var args;
  args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
  return couchreq.apply(null, ['DELETE'].concat(__slice.call(args)));
};

select = function(db, expr, errcb, cb) {
	var data;
	data = {
		map: "function(doc){with(doc){if(" + expr + "){emit(null, doc);}}}"
	};
	return couchreq.post(db, '_temp_view', data, function(err, body) {
		if (err) {
			return errcb(err);
		} else {
			return cb(body.rows);
		}
	});
};

/*
# Actual handlers
*/

handle(/insert into (\S+) value (.*)/, function(db, expr, cb) {
  try {
	 expr = (function() {
		return eval("(" + expr + ")");
	 })();
  } catch (e) {
	 cb("expression isn't valid JSON");
	 return;
  }
  return couchreq.post(db, expr, cb);
});

handle(/insert into (\S+) select (.*)/, function(db, selectExpr, cb) {
  return processExpr("select " + selectExpr, function(err, docs) {
	 if (err) {
		return cb(err);
	 } else {
		return couchreq.post(db, '_bulk_docs', {
		  docs: docs
		}, cb);
	 }
  });
});

handle(/select (.*? )?from (\S+)(?: where (.*))?/, function(outexpr, db, expr, cb) {
  if (expr == null) {
	 expr = true;
  }
  return select(db, expr, cb, function(rows) {
	 var result, results, row, _i, _len;
	 results = [];
	 for (_i = 0, _len = rows.length; _i < _len; _i++) {
		row = rows[_i].value;
		result = null;
		if (outexpr) {
		  try {
			 with(row){result = (function(){return eval("("+outexpr+")");})()};
		  } catch (e) {
			 cb(e.message);
			 return;
		  }
		  null;
		} else {
		  result = row;
		}
		results.push(result);
	 }
	 return cb(null, results);
  });
});

handle(/update (\S+) set (.*?)(?: where (.*))?/, function(db, updateExpr, expr, cb) {
  if (expr == null) {
	 expr = true;
  }
  updateExpr = "var " + updateExpr;
  return burrito(updateExpr, function(node) {
	 var vars;
	 if (node.name === 'var') {
		vars = node.label();
		return select(db, expr, cb, function(rows) {
		  var row, updates, v, _i, _j, _len, _len1;
		  updates = [];
		  for (_i = 0, _len = rows.length; _i < _len; _i++) {
			 row = rows[_i].value;
			 for (_j = 0, _len1 = vars.length; _j < _len1; _j++) {
				v = vars[_j];
				row[v] = null;
			 }
			 try {
				(function(){with(row){eval(updateExpr);}})();
			 } catch (e) {
				cb("bad update expression: " + e.message);
				return;
			 }
			 updates.push(row);
		  }
		  return couchreq.post(db, '_bulk_docs', {
			 docs: updates
		  }, cb);
		});
	 }
  });
});

handle(/delete from (\S+)(?: where (.*))?/, function(db, expr, cb) {
  var query;
  query = "update " + db + " set _deleted = true";
  if (expr) {
	 query += " where " + expr;
  }
  return processExpr(query, cb);
});

handle(/create collection (\S+)/, function(db, cb) {
  return couchreq.put(db, {}, cb);
});

handle(/drop collection (\S+)/, function(db, cb) {
  return couchreq["delete"](db, {}, cb);
});

handle(/show collections/, function(cb) {
  return couchreq.get('_all_dbs', {}, cb);
});

handle(/use (\S+)/, function(newurl, cb) {
  couchURL = newurl.replace(/\/$/, '');
  if (!couchURL.match("://")) {
	 couchURL = "http://" + couchURL;
  }
  couchAuth = null;
  return cb(null, {
	 ok: true
  });
});

handle(/select (.*)/, function(expr, cb) {
  try {
	 cb(null, (function() {
		return eval("(" + expr + ")");
	 })());
	 return null;
  } catch (e) {
	 return cb(e.message);
  }
});
