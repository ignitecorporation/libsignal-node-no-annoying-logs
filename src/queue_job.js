// vim: ts=4:sw=4:expandtab

/*
 * jobQueue manages multiple queues indexed by device to serialize
 * session io ops on the database.
 */
'use strict';


const _queueAsyncBuckets = new Map();
const _gcLimit = 10000;

async function _asyncQueueExecutor(queue, cleanup) {
    let offt = 0;
    while (true) {
        let limit = Math.min(queue.length, _gcLimit); // Break up thundering hurds for GC duty.
        for (let i = offt; i < limit; i++) {
            const job = queue[i];
            try {
                job.resolve(await job.awaitable());
            } catch(e) {
                job.reject(e);
            }
        }
        if (limit < queue.length) {
            /* Perform lazy GC of queue for faster iteration. */
            if (limit >= _gcLimit) {
                queue.splice(0, limit);
                offt = 0;
            } else {
                offt = limit;
            }
        } else {
            break;
        }
    }
    cleanup();
}

module.exports = function(bucket, awaitable) {
    /* Run the async awaitable only when all other async calls registered
     * here have completed (or thrown).  The bucket argument is a hashable
     * key representing the task queue to use. */
    if (!awaitable.name) {
        // Make debuging easier by adding a name to this function.
        Object.defineProperty(awaitable, 'name', {writable: true});
        if (typeof bucket === 'string') {
            awaitable.name = bucket;
        } else {
            console.warn("Unhandled bucket type (for naming):", typeof bucket, bucket);
        }
    }
    let inactive;
    if (!_queueAsyncBuckets.has(bucket)) {
        _queueAsyncBuckets.set(bucket, []);
        inactive = true;
    }
    const queue = _queueAsyncBuckets.get(bucket);
    const job = new Promise((resolve, reject) => queue.push({
        awaitable,
        resolve,
        reject
    }));
    if (inactive) {
        /* An executor is not currently active; Start one now. */
        _asyncQueueExecutor(queue, () => _queueAsyncBuckets.delete(bucket));
    }
    return job;
};


// const queues = new Map();
// const GC_LIMIT = 1000;
// const BATCH_SIZE = 100;

// async function asyncQueueExecutor(bucket) {
//     const queue = queues.get(bucket);
//     if (!queue) return;

//     while (queue.length > 0) {
//         const batch = queue.splice(0, BATCH_SIZE);
//         await Promise.all(batch.map(async (job) => {
//             try {
//                 const result = await job.awaitable();
//                 job.resolve(result);
//             } catch (error) {
//                 job.reject(error);
//             }
//         }));
//         // Liberar o event loop
//         await new Promise(resolve => setImmediate(resolve));
//     }

//     queues.delete(bucket);
// }

// function queueJob(bucket, awaitable) {
//     return new Promise((resolve, reject) => {
//         const job = { awaitable, resolve, reject };
//         if (!queues.has(bucket)) {
//             queues.set(bucket, []);
//             process.nextTick(() => asyncQueueExecutor(bucket));
//         }
//         const queue = queues.get(bucket);
//         queue.push(job);

//     });
// }

// module.exports = queueJob;