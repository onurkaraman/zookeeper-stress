import argparse
import kazoo
import kazoo.client
import time

def build_watcher(client, path):
  @client.DataWatch(path)
  def func(data, stat):
    pass

def maybe_create(client, path):
  try:
    client.create(path)
  except kazoo.exceptions.NodeExistsError:
    pass

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='stress zookeeper with znodes and watchers')
  parser.add_argument('--ensemble', type=str, required=True, help='list of zookeeper machines ex: host1:port1,host2:port2')
  parser.add_argument('--count', type=int, required=True, help='number of znodes to create or watch')
  parser.add_argument('--root', type=str, default='/', help='root znode containing the znodes to create or watch')
  parser.add_argument('--mode', type=str, required=True, choices=['create', 'watch', 'both'], help='create znodes, watch znodes, or both')
  args = parser.parse_args()
  client = kazoo.client.KazooClient(args.ensemble)
  client.start()
  maybe_create(client, args.root)
  for i in range(args.count):
    path = '%s/%d' % (args.root, i)
    if args.mode in ['create', 'both']:
      maybe_create(client, path)
    if args.mode in ['watch', 'both']:
      build_watcher(client, path)
  while True:
    time.sleep(1)
