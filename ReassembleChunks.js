const { BlobServiceClient } = require('@azure/storage-blob');
const stream = require('stream');

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const containerName = process.env.ContainerName;
const finalBlobName = process.env.FinalBlobName;
const CHUNK_SIZE = 1024 * 1024; // 1 MB

module.exports = async function (context, myTimer) {
    const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
    const containerClient = blobServiceClient.getContainerClient(containerName);

    const blobs = [];
    for await (const blob of containerClient.listBlobsFlat()) {
        if (blob.name.startsWith('video-chunk-')) {
            blobs.push(blob.name);
        }
    }

    blobs.sort((a, b) => {
        const aIndex = parseInt(a.split('-')[2], 10);
        const bIndex = parseInt(b.split('-')[2], 10);
        return aIndex - bIndex;
    });

    const finalBlobClient = containerClient.getBlockBlobClient(finalBlobName);
    const writableStream = new stream.PassThrough();

    finalBlobClient.uploadStream(writableStream, CHUNK_SIZE, 5, {
        onProgress: (ev) => console.log(`Upload progress: ${ev.loadedBytes} bytes`),
    });

    for (const blobName of blobs) {
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        const downloadBlockBlobResponse = await blockBlobClient.download();
        const chunkStream = downloadBlockBlobResponse.readableStreamBody;
        chunkStream.pipe(writableStream, { end: false });

        chunkStream.on('end', () => {
            console.log(`Chunk ${blobName} downloaded and piped`);
        });
    }

    writableStream.end();
    console.log(`Final video reassembled as ${finalBlobName}`);
};
