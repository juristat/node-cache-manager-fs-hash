const {promisify} = require('es6-promisify');
const fs = require('fs');

function metaFilePath(cachePath) {
	return cachePath + '/meta.json';
}

async function readMetaFile(cachePath) {
	try {
		const fileContents = await promisify(fs.readFile)(metaFilePath(cachePath));
		const metadata = JSON.parse(fileContents);
		return metadata;
	} catch (err) {
		// Return the default meta file when none is found
		if (err.code === 'ENOENT') {
			return { size: 0, entries: {} };
		}

		// If the meta file is somehow not valid json, throw that error up
		if (err instanceof SyntaxError) {
			throw new Error('fs-cache meta file is not valid json');
		}

		// Re-throw all other errors
		throw err;
	}
}

async function writeMetaFile(cachePath, metadata) {
	const metadataString = JSON.stringify(metadata);
	await promisify(fs.writeFile)(metaFilePath(cachePath), metadataString, 'utf8');
}

async function reduceCacheSize(metadata, bytesToReduce) {
	const entriesByDate = Object.values(metadata.entries).sort((a, b) => {
		if (a.created < b.created) return -1;
		if (a.created > b.created) return 1;
		return 0;
	});

	const entriesToDelete = [];
	let sizeDeleted = 0;
	while (sizeDeleted < bytesToReduce) {
		const entry = entriesByDate.shift();
		sizeDeleted += entry.size;
		entriesToDelete.push(entry);
		delete metadata.entries[entry.path];
		metadata.size -= entry.size;
	}

	await Promise.all(entriesToDelete.map(entry => exports.delete(entry.path, {})));

	return metadata;
}

exports.write = async function (path, data, { path: cachePath, maxsize }) {
    const externalBuffers = [];
	let externalBuffersLength = 0;
    const dataString = JSON.stringify(data, function replacerFunction(k, value) {
        //Buffers searilize to {data: [...], type: "Buffer"}
        if (value && value.type === 'Buffer' && value.data && value.data.length >= 1024 /* only save bigger Buffers external, small ones can be inlined */) {
            const buffer = Buffer.from(value.data);
			externalBuffersLength += buffer.length;
            externalBuffers.push({
                index: externalBuffers.length,
                buffer: buffer,
            });
            return {
                type: 'ExternalBuffer',
                index: externalBuffers.length - 1,
                size: buffer.length,
            };
        } else {
            return value;
        }
    });

	// If we try to set a single cache item bigger than the allowable cache size
	const newDataLength = dataString.length + externalBuffersLength;
	if (newDataLength > maxsize) {
		throw new Error(`not setting cache value ${data.key}, value is length ${newDataLength} and maxsize is ${maxsize}`);
	}

	const metadata = await readMetaFile(cachePath);

	// if we need to reduce the cache size before setting
	const newCacheSize = metadata.size + newDataLength;
	if (newCacheSize > maxsize) {
		await reduceCacheSize(metadata, newCacheSize - maxsize);
	}

    //save main json file
    await promisify(fs.writeFile)(path + '.json', dataString, 'utf8');

	// update the metadata file with the new size info
	metadata.size += newDataLength;
	metadata.entries[path] = {
		path, 
		size: newDataLength,
		created: Date.now(),
	};
	await writeMetaFile(cachePath, metadata);

    //save external buffers
    await Promise.all(externalBuffers.map(async function (externalBuffer) {
        await promisify(fs.writeFile)(path + '-' + externalBuffer.index + '.bin', externalBuffer.buffer, 'utf8');
    }));
};


exports.read = async function (path) {
    //read main json file
    const dataString = await promisify(fs.readFile)(path + '.json', 'utf8');


    const externalBuffers = [];
    const data = JSON.parse(dataString, function bufferReceiver(k, value) {
        if (value && value.type === 'Buffer' && value.data) {
            return Buffer.from(value.data);
        } else if (value && value.type === 'ExternalBuffer' && typeof value.index === 'number' && typeof value.size === 'number') {
            //JSON.parse is sync so we need to return a buffer sync, we will fill the buffer later
            const buffer = Buffer.alloc(value.size);
            externalBuffers.push({
                index: +value.index,
                buffer: buffer,
            });
            return buffer;
        } else {
            return value;
        }
    });

    //read external buffers
    await Promise.all(externalBuffers.map(async function (externalBuffer) {
        const fd = await promisify(fs.open)(path + '-' + +externalBuffer.index + '.bin', 'r');
        await promisify(fs.read)(fd, externalBuffer.buffer, 0, externalBuffer.buffer.length, 0);
        await promisify(fs.close)(fd);
    }));
    return data;
};

exports.delete = async function (path, { path: cachePath }) {
    await promisify(fs.unlink)(path + '.json');

    //delete binary files
    try {
        for (let i = 0; i < Infinity; i++) {
            await promisify(fs.unlink)(path + '-' + i + '.bin');
        }
    } catch (err) {
        if (err.code === 'ENOENT') {
            // every binary is deleted, we are done
        } else {
            throw err;
        }
    }

	// When this is being run from the size reducer function, we won't include the path.
	// The reducer function will take care of removing entries from the metadata.
	if (cachePath) {
		const metadata = await readMetaFile(cachePath);
		const entrySize = metadata.entries[path].size;
		metadata.size -= entrySize;
		await writeMetaFile(cachePath, metadata);
	}
};
