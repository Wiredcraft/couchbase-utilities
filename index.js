"use strict";

const Couchbase = require("couchbase");
const { ViewQuery, N1qlQuery } = Couchbase;
const debug = require("debug")("cb-utils:index");

const noop = function () {};
const delay = (ms) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, ms);
  });
};

function connectCluster(host, username, password) {
  const cluster = new Couchbase.Cluster(host);
  cluster.authenticate(username, password);
  return cluster;
}

/**
 * open bucket
 * @param {Couchbase.Cluster} cluster
 * @param {string} bucket
 * @returns {Promise<Couchbase.Bucket>}
 */
function openBucket(cluster, bucket) {
  return new Promise((resolve, reject) => {
    const result = cluster.openBucket(bucket, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
}

function getDoc(bucket, key) {
  return new Promise((resolve, reject) => {
    bucket.get(key, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
}

function upsert(bucket, key, value) {
  return new Promise((resolve, reject) => {
    bucket.upsert(key, value, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res);
      }
    });
  });
}

async function upsertViews(bucket, designDocs) {
  debug("bucket.name %s", bucket.name);
  const ddocs = designDocs.filter((ddoc) => {
    return ddoc.bucket === bucket.name;
  });
  if (ddocs.length === 0) return;
  debug("ddocs %o", ddocs);
  await Promise.all(
    ddocs.map((ddoc) => {
      return new Promise((resolve, reject) => {
        console.log("[info] upsertDesignDocument:", ddoc.docName);
        bucket.manager().upsertDesignDocument(ddoc.docName, ddoc.doc, (err) => {
          if (err) {
            return reject(err);
          }
          return resolve();
        });
      });
    })
  );
  return;
}

function newViewQuery(designDoc, viewName, options) {
  const ViewQuery = Couchbase.ViewQuery;
  // With some defaults.
  // See https://docs.couchbase.com/sdk-api/couchbase-node-client-2.1.4/ViewQuery.html#.ErrorMode
  let query = ViewQuery.from(designDoc, viewName)
    .on_error(ViewQuery.ErrorMode.STOP)
    .order(ViewQuery.Order.ASCENDING)
    .stale(ViewQuery.Update.BEFORE);

  if (options == null) {
    return query;
  }
  // The SDK made it easier for some options formats.
  // Call.
  let opts = Object.assign({}, options);
  [
    "stale",
    "order",
    "group",
    "group_level",
    "key",
    "keys",
    "include_docs",
    "full_set",
    "on_error",
    "limit",
  ].forEach((key) => {
    if (opts[key] != null) {
      query = query[key].call(query, opts[key]);
      delete opts[key];
    }
  });
  // Apply.
  ["range", "id_range"].forEach((key) => {
    if (opts[key] != null) {
      query = query[key].apply(query, opts[key]);
      delete opts[key];
    }
  });
  query = query.custom(opts);
  return query;
}

/**
 * query bucket
 * @param {Couchbase.Bucket} bucket
 * @param {any} query
 * @returns {Promise<{rows: any[], meta: Couchbase.Bucket.N1qlQueryResponse.Meta}>}
 */
async function query(bucket, q) {
  return new Promise((resolve, reject) => {
    bucket.query(q, (err, rows, meta) => {
      if (err) {
        reject(err);
      } else {
        resolve({ rows, meta });
      }
    });
  });
}

// alias of query
async function queryView(bucket, q) {
  return (await query(bucket, q)).rows;
}

async function queryN1ql(bucket, q) {
  return query(bucket, N1qlQuery.fromString(q));
}

const paginate = async (
  bucket,
  designDoc,
  view,
  viewOpts,
  exec = noop,
  startkey_docid
) => {
  if (startkey_docid) {
    viewOpts.stale = ViewQuery.Update.NONE;
    viewOpts.id_range = [startkey_docid];
    viewOpts.skip = 1;
  }
  const q = newViewQuery(designDoc, view, viewOpts);
  const queryResult = await queryView(bucket, q);
  const startKey = viewOpts.range[0];
  const res = queryResult.filter((r) => r.key === startKey);
  const stopRecur = res.length < viewOpts.limit;

  debug("selected doc length: %d \n", res.length);
  const ids = res.map((r) => r.id);
  await Promise.all(
    ids.map(async (id) => {
      return exec(bucket, id);
    })
  );
  debug("finished callback");
  if (!stopRecur) {
    const lag = 200;
    console.log(`rest for ${lag} millseconds`);
    await delay(lag);
    await paginate(
      bucket,
      designDoc,
      view,
      viewOpts,
      exec,
      ids[ids.length - 1]
    );
  } else {
    console.log("pagination done");
    return;
  }
};
const groupBy = function (col, key) {
  return col.reduce(function (acc, cur) {
    (acc[cur[key]] = acc[cur[key]] || []).push(cur);
    return acc;
  }, {});
};

async function defineIndexes(buckets, indexes) {
  const indexDefinitions = indexes.map(async (idx) => {
    const text = idx.template
      .replace(new RegExp(`\\$bucket`, "g"), idx.bucket)
      .replace(new RegExp(`\\$name`, "g"), idx.name)
      .replace(
        new RegExp(`\\$defered`, "g"),
        idx.defer_build ? "true" : "false"
      );
    console.log("[info]", text);
    try {
      await query(buckets[idx.bucket], N1qlQuery.fromString(text));
    } catch (err) {
      if (err.code === 4300) {
        console.warn("[warn]", err.message + " Trying to drop it.");
        const dropIndex = `DROP INDEX \`${idx.bucket}\`.${idx.name} using GSI`;
        await query(buckets[idx.bucket], N1qlQuery.fromString(dropIndex));
        await query(buckets[idx.bucket], N1qlQuery.fromString(text));
        console.log("[info]", `GSI index ${idx.name} created`);
      } else {
        throw err;
      }
    }
  });
  await Promise.all(indexDefinitions);
}

async function triggerDeferedIndexes(buckets, indexes) {
  const indexesOnBucket = groupBy(indexes, "bucket");
  debug("indexesOnBucket %o", indexesOnBucket);
  return Promise.all(
    Object.values(indexesOnBucket).map(async (idxArr) => {
      const bucketName = idxArr[0].bucket;
      const indexNames = idxArr.map((i) => "`" + i.name + "`").join(",");
      const text = `build index on \`${bucketName}\`(${indexNames}) using gsi`;
      console.log("[info]", text);
      try {
        await query(buckets[bucketName], N1qlQuery.fromString(text));
      } catch (err) {
        if (err.message.includes("is already built")) {
          console.log("[warn]", err.message);
        } else {
          throw err;
        }
      }
    })
  );
}

async function buildIndexes(buckets, indexes) {
  await defineIndexes(buckets, indexes);

  const defered = indexes.filter((idx) => idx.defer_build);
  if (defered.length <= 0) {
    console.log(`[info] no deferred indexes`);
    return;
  }
  await triggerDeferedIndexes(buckets, defered);
}

module.exports = {
  connectCluster,
  openBucket,
  newViewQuery,
  queryView,
  queryN1ql,
  query,
  ViewQuery,
  getDoc,
  paginate,
  upsert,
  upsertViews,
  buildIndexes,
};
