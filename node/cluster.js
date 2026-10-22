import cluster from 'node:cluster';

if (cluster.isPrimary) {
  console.log('Starting two gRPC server processes');
  cluster.fork();
  cluster.fork();
} else {
  await import('./server.js');
}
