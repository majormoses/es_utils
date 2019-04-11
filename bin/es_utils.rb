#!/usr/bin/env ruby
# frozen_string_literal: true

require 'thor'
require 'aws-sdk'
require 'elasticsearch'
require 'httparty'
require 'colorize'
require 'net/ssh'
require 'humanize-bytes'
require 'highline'
# require 'socket'

# for some operations it might be more performant to use
# a non standard adapter for more peristent connections say during a migration
# TODO: make this something conditional based on the calls made
# require 'typhoeus'
# require 'typhoeus/adapters/faraday'

class ESUtils < Thor # rubocop:disable Metrics/ClassLength:
  desc 'es_responds', 'makes sure that es is responsing on port specified'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  def es_responds(host = nil, port = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    response = HTTParty.get("http://#{host}:#{port}", timeout: 1)
    return true if response.code == 200
  rescue Net::OpenTimeout, Errno::ECONNREFUSED
    sleep 1
    return false
  end
  desc 'wait_es_responds', 'waits until es is responsing on port specified'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  def wait_es_responds(host = nil, port = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    p 'waiting for ES to respond: '
    until es_responds(host, port)
      print '.'.yellow
      sleep 1
    end
    p 'ES returned 200'
  end

  desc 'es_version', 'returns the version of an ES cluster'
  method_option :host,
                aliases: '-h',
                desc: 'A host ip/fqdn in the cluster to check',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'The port ES listens on',
                default: 9200

  def es_version(host = nil, port = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    wait_es_responds(host, port)
    response = HTTParty.get("http://#{host}:#{port}", timeout: 1)
    version = response.parsed_response['version']['number']
    p version
    version
  end

  desc 'index_exists?', 'returns true if index exists'
  method_option :host,
                aliases: '-h',
                desc: 'A host ip/fqdn in the cluster to check',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'The port ES listens on',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'index name, assumes no prefix',
                required: true

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def index_exists?(host, port, index, verbose)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    verbose = options[:verbose] if verbose.nil?

    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    client.indices.exists? index: index
  end

  desc 'create_index', 'creates an index with optional settings'
  method_option :host,
                aliases: '-h',
                desc: 'ES host',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'index name, can be used with prefix',
                required: true

  method_option :index_prefix,
                desc: 'prefix to combine with the index name'

  method_option :shard_count,
                aliases: '-s',
                desc: 'number of shards to create the index with',
                default: 5 # matches ES defaults

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def create_index(host = nil, port = 9200, index = nil, index_prefix = nil, shard_count = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    index_prefix = options[:index_prefix] if index_prefix.nil?
    shard_count = options[:shard_count] if shard_count.nil?
    verbose = options[:verbose] if verbose.nil?

    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )

    if index_exists?(host, port, "#{index_prefix}#{index}", verbose)
      p "index: #{index_prefix}#{index} already exists skipping"
    else
      client.indices.create index: "#{index_prefix}#{index}",
                            body: {
                              settings: {
                                number_of_replicas: 0, # fast for migrations, might expose later
                                number_of_shards: shard_count
                              }
                            }
    end
  end

  desc 'create_indices', 'creates multiple indices'
  method_option :host,
                aliases: '-h',
                desc: 'ES host',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :indices,
                aliases: '-i',
                desc: 'a comma seperated list of indices, can be used with prefix',
                required: true

  method_option :indices_prefix,
                aliases: '-I',
                desc: 'prefix to combine with the indices, all or nothing not per index',
                default: nil

  method_option :shard_count,
                aliases: '-s',
                desc: 'a comma seperated list of counts for each indices'

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def create_indices(host = nil, port = 9200, indices = nil, indices_prefix = nil, shard_count = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    indices = options[:indices] if indices.nil?
    indices_prefix = options[:indices_prefix] if indices_prefix.nil?
    shard_count = options[:shard_count] if shard_count.nil?
    verbose = options[:verbose] if verbose.nil?

    indices = if indices.is_a?(Array)
                indices
              elsif indices.is_a?(String)
                indices.split(',')
              else
                raise TypeError, "indices: must be a string or array, you passed #{indices}"
              end

    shard_count = if shard_count.is_a?(Array)
                    shard_count
                  elsif shard_count.is_a?(String)
                    shard_count.split(',')
                  else
                    raise TypeError, "shards: must be a string or array, you passed #{shard_count}"
                  end
    count = 0
    indices.each do |index|
      # ensure that all indices have shard count, use ES default if needed
      if shard_count.nil?
        shard_count.insert(count, 5)
      end

      create_index(
        host,
        port,
        index,
        indices_prefix,
        shard_count[count],
        verbose
      )
    end
  end

  desc 'get_init_shards', 'returns initializing shards'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def get_init_shards(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    shards = client.cat.shards(h: 'index,shard,prirep,state,node').split("\n")
    initializing_shards = []
    shards.each do |s|
      initializing_shards << s.squeeze(' ') if s.include? 'INITIALIZING'
    end
    if initializing_shards.empty?
      p 'no initializing_shards'
    else
      p 'initializing_shards'
      p initializing_shards
    end
    initializing_shards
  end

  desc 'get_unallocated_shards', 'returns a list of unallocated shards'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false
  def get_unallocated_shards(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    shards = client.cat.shards(h: 'index,shard,prirep,state,node', v: true).split(/\n+/)
    return [] if shards.length <= 1

    # convert header row to array
    key_arr = shards[0].split

    # convert value rows to arrays
    values_arr = shards[1..shards.length].map(&:split)

    # make an array of objects from key array and value arrays
    shards = values_arr.map { |v| key_arr.zip(v).to_h }.compact

    unallocated_shards = []
    shards.each do |s|
      unallocated_shards << s if s['state'] == 'UNASSIGNED'
    end
    if !unallocated_shards.empty?
      p 'unallocated shards list'
      p unallocated_shards
    else
      p 'no unallocated shard'
    end
    unallocated_shards
  end
  desc 'move_shard', 'move a shard'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'The es index name',
                required: true

  method_option :shard,
                aliases: '-s',
                desc: 'The es shard number for the respective index',
                required: true

  method_option :from_node,
                aliases: '-t',
                desc: 'The es node name to move a shard from',
                required: true

  method_option :to_node,
                aliases: '-d',
                desc: 'The es node name to move a shard to',
                required: true

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def move_shard(host = nil, port = nil, index = nil, shard = nil, from_node = nil, to_node = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    shard = options[:shard] if shard.nil?
    from_node = options[:from_node] if from_node.nil?
    to_node = options[:to_node] if to_node.nil?
    verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    client.cluster.reroute body: {
      commands: [
        {
          move: {
            index: index,
            shard: shard,
            from_node: from_node,
            to_node: to_node
          }
        }
      ]
    }
  end
  desc 'allocate_shard', 'allocates a shard'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'The es index name',
                required: true

  method_option :shard,
                aliases: '-s',
                desc: 'The es shard number for the respective index',
                required: true

  method_option :node,
                aliases: '-n',
                desc: 'The es node name to allocate on',
                required: true

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def allocate_shard(host = nil, port = nil, index = nil, shard = nil, node = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    shard = options[:shard] if shard.nil?
    node = options[:node] if node.nil?
    begin
      verbose = options[:verbose] if verbose.empty?
    rescue StandardError
      FalseClass
    end
    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(host: host, port: port, log: verbose)
    client.cluster.reroute body: {
      commands: [
        {
          allocate_replica: {
            index: index,
            shard: shard.to_i,
            node: node,
            allow_primary: true
          }
        }
      ]
    }
  end
  desc 'assign_unallocated_shards', 'allocates a list of unallocated shards'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'index to allocate, dont set this...'

  method_option :shard,
                aliases: '-s',
                desc: 'shard to allocate, dont set this...'

  method_option :node,
                aliases: '-n',
                desc: 'node to allocate to, dont set this...'

  method_option :attempts,
                aliases: '-a',
                desc: 'no of attempts to find out a node which does not hold same shard (replica)',
                default: 3

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def assign_unallocated_shards(host = nil, port = nil, index = nil, shard = nil, node = nil, attempts = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    shard = options[:shard] if shard.nil?
    node = options[:node] if node.nil?
    attempts = options[:attempts] if attempts.nil?
    verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    # we need a list of data nodes to spread the shards accross
    # rewrite the options to the appropriate role
    hosts = get_nodes(host, port, 'data')
    # make sure cluster shard allocation is enabled - default is enabled='all'
    cluster_shard_allocation(host, port, 'all', verbose: verbose)
    # wait for any relocating shards
    wait_for_relocating_shards(host, port)
    # sleep for 5 sec before checking for UNASSIGNED shards
    sleep(10)
    # get the list of unallocated_shards
    unallocated_shards = get_unallocated_shards(host, port)
    # for each unallocated_shard we need to choose a host at random
    unallocated_shards.each do |s|
      # trying 3 times incase if allocating shards end up in same node which has replica and could fail. however this will
      # not happen as rebalance/auto allocation will automatically relocate shard to other node. Just being cautious
      attempts.to_i.times do
        begin
          index = s.split(' ')[0]
          shard = s.split(' ')[1]
          node = hosts.sample
          allocate_shard(host, port, index, shard, node)
          sleep(1) # allowing time to allocate shards
          break
        rescue StandardError => e
          p "caught exception #{e}! ohnoes!"
        end
      end
    end
  end

  desc 'get_nodes', 'gets list of nodes in ES cluster'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :roles,
                aliases: '-r',
                desc: 'One or more (comma seperated list of roles)',
                default: 'master,data_nodes,current_master,client'
  # method_option :names, aliases: '-n', desc: 'returns_hostnames rather than ips', type: :boolean, default: false

  method_option :count,
                aliases: '-c',
                desc: 'display count of nodes in cluster (NOT_YET_IMPLEMENTED)',
                type: :boolean

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def get_nodes(host = nil, port = nil, roles = nil, names = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    roles = options[:roles] if roles.nil?
    names = options[:names] if names.nil?
    # count = options[:count] if count.nil?
    verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    cluster_list = client.cat.nodes(h: 'ip,name,m,node.role', v: true).split(/\n+/)
    return [] if cluster_list.length <= 1

    # convert header row to array
    key_arr = cluster_list[0].split

    # convert value rows to arrays
    values_arr = cluster_list[1..cluster_list.length].map(&:split)

    # make an array of objects from key array and value arrays
    cluster_list = values_arr.map { |v| key_arr.zip(v).to_h }.compact

    nodes = []
    master_nodes = []
    current_master = []
    data_nodes = []
    client_nodes = []
    index = if names
              'name'
            else
              'ip'
            end
    roles = roles.split(',')
    cluster_list.each do |node|
      # masters
      if node['node.role'] == 'm'
        master_nodes << node[index]
      end

      if node['m'] == '*'
        current_master << node[index]
      end

      # data nodes
      # I think it can contain dc and be a data + client node
      data_nodes << node[index] if node['node.role'].include? 'd'

      # Exclusive client nodes
      if node['node.role'].include?('-') && node['m'] == '-'
        client_nodes << node[index]
      end
    end
    nodes << master_nodes.sort if roles.include? 'master'
    nodes << current_master if roles.include? 'current_master'
    nodes << data_nodes.sort if roles.include? 'data'
    nodes << client_nodes.sort if roles.include? 'client'
    p nodes.flatten
    nodes.flatten
  end
  desc 'cluster_shard_allocation', 'toggles shard allocation accross cluster'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :status,
                aliases: '-d',
                desc: 'enabled/all (default) disabled/none',
                default: 'all'

  method_option :retries,
                aliases: '-r',
                desc: 'number of times to retry failures',
                default: nil

  method_option :sleep,
                aliases: '-s',
                desc: 'time to sleep between retries',
                default: 10

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def cluster_shard_allocation(host = nil, port = nil, status = nil, retries = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    status = options[:status] if status.nil?
    retries = options[:retries] if retries.nil?
    verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    p retries
    attempt = 0
    begin
      client = Elasticsearch::Client.new(
        host: host,
        port: port,
        log: verbose
      )
      client.cluster.put_settings body: { transient: { 'cluster.routing.allocation.enable' => status } }
    rescue Elasticsearch::Transport::Transport::Errors::InternalServerError => e
      attempt += 1
      p "attempts: #{attempt}, retries: #{retries}"
      if attempt == retries
        p "attempt #{attempt} to send allocation of: #{status} to cluster"
        raise e
      else
        retry
      end
    end
  end
  desc 'if_relocating_shards', 'returns true if there are unallocated shards'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def if_relocating_shards(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    if client.cluster.health['relocating_shards'].nonzero?
      return false
    else
      p 'no relocating shards in the cluster'
      return true
    end
  end
  desc 'wait_for_relocating_shards', 'returns false until all shards are allocated'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def wait_for_relocating_shards(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?
    p 'waiting for all shards to be relocated'
    until if_relocating_shards(host, port, verbose: verbose)
      print '.'
      sleep 1
    end
  end
  desc 'if_unassigned_shards', 'returns true if there are unallocated shards'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def if_unassigned_shards(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )
    if client.cluster.health['unassigned_shards'].nonzero?
      p 'There are unassigned shards in the cluster'
      return false
    else
      p 'no unassigned shards in the cluster'
      return true
    end
  end
  desc 'wait_for_assigned_shards', 'returns false until all shards are allocated'
  method_option :host,
                aliases: '-h',
                desc: 'ES host to query against',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'ES port',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def wait_for_assigned_shards(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?
    p 'waiting for all shards to be assigned'
    until if_unassigned_shards(host, port, verbose: verbose)
      print '.'
      sleep 1
    end
  end
  desc 'restart_es_service', 'ssh into an instance and restart the es_service'
  method_option :host,
                aliases: '-h',
                desc: 'host to ssh and restart',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'port ssh is running on',
                default: 22

  method_option :user,
                aliases: '-u',
                desc: 'username to ssh as',
                default: `whoami`.chomp

  method_option :key,
                aliases: '-k',
                desc: 'key',
                default: ('/home/' + options[:user].default + '/.ssh/id_rsa')

  method_option :converge,
                aliases: '-e', desc: 'converge node default/false',
                type: :boolean,
                default: false
  def restart_es_service(host = nil, user = nil, key = nil, converge = nil)
    host = options[:host] if host.nil?
    user = options[:user] if user.nil?
    key = options[:key] if key.nil?
    converge = options[:converge] if converge.nil?
    stdout = ''
    Net::SSH.start(host, user, keys: key) do |ssh|
      # capture all stderr and stdout output from a remote process
      output = ssh.exec!('hostname')
      print output
      ssh.exec!('sudo service elasticsearch stop') do |_channel, stream, data|
        stdout << data if stream == :stdout
      end
      # if converge flag is enabled
      if converge
        ssh.exec!('sudo chef-client') do |_channel, stream, data|
          stdout << data if stream == :stdout
        end
      end
      ssh.exec!('sudo service elasticsearch start') do |_channel, stream, data|
        stdout << data if stream == :stdout
      end
    end
    print stdout
  end

  desc 'index_safe', 'checks if an index recovery is done'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'the index to modify'

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                default: false

  def index_safe(host = nil, port = nil, index = nil, verbose = false)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = index[:index] if index.nil?
    begin
      verbose = options[:verbose] if verbose.empty?
    rescue StandardError
      FalseClass
    end
    client = Elasticsearch::Client.new(host: host, port: port, verbose: verbose)
    i = client.cat.recovery(index: index)
    status = i.split(' ')[4]
    if status == 'done'
      p 'recovery done!'
      return true
    else
      return false
    end
  end

  desc 'wait_index_safe', 'checks if an index recovery is done'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'the index to modify'

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                default: false

  def wait_index_safe(host = nil, port = nil, index = nil, verbose = false)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    begin
      verbose = options[:verbose] if verbose.empty?
    rescue StandardError
      FalseClass
    end
    p 'waiting for all shards to be recovered'
    until index_safe(host, port, index, verbose: verbose)
      print '.'
      sleep 1
    end
  end

  desc 'edit_index_replicas', 'changes the number of replicas for a given index'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'the index to modify'

  method_option :num_replicas,
                aliases: '-r',
                desc: 'the number of replicas for the index',
                default: 1

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false
  def edit_index_replicas(host = nil, port = nil, index = nil, num_replicas = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    num_replicas = options[:num_replicas] if num_replicas.nil?
    begin
      verbose = options[:verbose] if verbose.empty?
    rescue StandardError
      FalseClass
    end
    # never edit the replica count unless its done recovering!
    wait_index_safe(host, port, index, verbose)
    # we should be safe now
    p "number of replicas desired: #{num_replicas}"
    # initialize client
    client = Elasticsearch::Client.new(host: host, port: port, verbose: verbose)
    if index == '_all'
      p "will be changing num_replicas cluster wide to #{num_replicas}...this will generate a lot of traffic if you increased it..."
      client.indices.put_settings body: {
        'number_of_replicas' => num_replicas.to_s
      }
      p 'change has been made'
    else
      settings = client.indices.get_settings index: index
      settings = settings[index]['settings']['index']
      if settings['number_of_replicas'].to_i == num_replicas.to_i
        p "Already have: #{num_replicas} replica(s)"
      else
        p "will be changing num_replicas from #{settings['number_of_replicas']} to #{num_replicas}"
        client.indices.put_settings index: index, body: {
          'number_of_replicas' => num_replicas.to_s
        }
        p 'change has been made'
      end
    end
  end

  # https://www.elastic.co/guide/en/elasticsearch/reference/current/delayed-allocation.html#delayed-allocation
  desc 'edit_delayed_allocation', 'adjusts delayed_allocation'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :delayed_timeout,
                aliases: '-d',
                desc: 'the number of minutes for delayed allocation',
                default: 1

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false
  def edit_delayed_allocation(host = nil, port = nil, _num_replicas = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    delayed_timeout = options[:delayed_timeout] if delayed_timeout.nil?
    begin
      verbose = options[:verbose] if verbose.empty?
    rescue StandardError
      FalseClass
    end
    # initialize client
    client = Elasticsearch::Client.new(host: host, port: port, verbose: verbose)
    # we should be safe now
    p "delayed_timeout: #{delayed_timeout}"
    # get a random index to get settings for
    indices = (client.cat.indices h: ['index']).tr(' ', '').split("\n")
    index = indices.sample
    p "random index selected: #{index}"
    settings = client.indices.get_settings index: index
    settings = settings[index]['settings']['index']['unassigned']
    if settings['node_left']['delayed_timeout'] == "#{delayed_timeout}m"
      p "Already have: #{delayed_timeout}m set"
    else
      p "will be updating #{settings['node_left']['delayed_tieout']} to #{delayed_timeout}m"
      client.indices.put_settings body: {
        'index.unassigned.node_left.delayed_timeout' => "#{delayed_timeout}m"
      }
    end
  end

  desc 'edit_index_recovery_priority', 'sets a index priority for a given index'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :reset,
                aliases: '-r',
                desc: 'if you wish to set all back to default of "1"',
                type: :boolean,
                default: false

  method_option :prompt,
                aliases: '-y',
                desc: 'setting this will prevent prompts to confirm changes',
                type: :boolean,
                default: false

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false
  def edit_index_recovery_priority(host = nil, port = nil, reset = nil, prompt = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    begin
      reset = options[:reset] if reset.nil?
      prompt = options[:prompt] if prompt.nil?
      verbose = options[:verbose] if verbose.nil?
    rescue StandardError
      FalseClass
    end

    client = Elasticsearch::Client.new(host: host, port: port, verbose: verbose)
    indices = (client.cat.indices h: ['index']).tr(' ', '').split("\n")
    indices.each do |index|
      store_stats = client.indices.stats index: index, store: true
      bytes = Humanize::Byte.new(store_stats['_all']['primaries']['store']['size_in_bytes'])
      gigs = bytes.to_g.value.round(2)

      # get the existing priority
      index_settings = client.indices.get_settings index: index
      currrent_priority = index_settings[index]['settings']['index']['priority'].to_i

      # set somewhat arbitrary priority
      default_priority = 1
      priority = if gigs >= 1000
                   default_priority
                 elsif gigs >= 500
                   default_priority * 2
                 elsif gigs >= 100
                   default_priority * 4
                 elsif gigs >= 50
                   default_priority * 8
                 elsif gigs >= 25
                   default_priority * 10
                 elsif gigs >= 12.5
                   default_priority * 12
                 elsif gigs >= 6.25
                   default_priority * 14
                 elsif gigs >= 3.13
                   default_priority * 16
                 elsif gigs >= 1.56
                   default_priority * 18
                 else
                   default_priority * 20
                 end
      if reset
        priority = default_priority
      end

      es_body = { 'index.priority' => priority }

      # modify priority as needed
      if currrent_priority.zero? # nil.to_i == 0
        p "[LOG_LEVEL=INFO] [timestamp=#{Time.now}] [index=#{index}] [size_in_gbs=#{gigs}] [message=index has default priority and has not been explicity set]"
        client.indices.put_settings index: index.to_s,
                                    body: es_body
        p "[LOG_LEVEL=INFO] [timestamp=#{Time.now}] [index=#{index}] [size_in_gbs=#{gigs}] [message=index has a new priority of: #{priority}]"
      elsif currrent_priority == priority
        p "[LOG_LEVEL=INFO] [timestamp=#{Time.now}] [index=#{index}] [size_in_gbs=#{gigs}] [message=index has the appropriate priority of #{priority} set. This is not the priority you are looking for.]"
      else
        p "[LOG_LEVEL=WARN] [timestamp=#{Time.now}] [index=#{index}] [size_in_gbs=#{gigs}] [message=index has a currrent_priority of: #{currrent_priority} and we think it should be: #{priority}]"
        unless prompt
          exit unless HighLine.new.agree('This will update the index recovery priority. Do you want to proceed?')
        end
        client.indices.put_settings index: index.to_s,
                                    body: es_body
        p "[LOG_LEVEL=WARN] [timestamp=#{Time.now}] [index=#{index}] [size_in_gbs=#{gigs}] [message=index was set to: #{priority}]"
      end
    end
  end

  desc 'delete_index', 'deletes an es index'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :index,
                aliases: '-i',
                desc: 'the index to delete'

  method_option :index_prefix,
                aliases: '-r',
                desc: 'the prefix to use to append to make an actual index name',
                default: nil

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def delete_index(host = nil, port = nil, index = nil, index_prefix = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    index = options[:index] if index.nil?
    index_prefix = options[:index_prefix] if index_prefix.nil?
    begin
      verbose = options[:verbose] if verbose.nil?
    rescue StandardError
      FalseClass
    end
    if index_prefix.nil? || index_prefix.empty?
      p 'no prefix specified, taking index as given'
    elsif index.include? '*'
      raise "not safe to use '*' in a index delete...use something to generate a list"
    else
      index = index_prefix.to_s + index.to_s
    end
    client = Elasticsearch::Client.new(host: host, port: port, verbose: verbose)
    begin
      p '[INFO] waiting for ES to be ready to handle the request'
      wait_es_responds(host, port)
      p "[INFO] deleting index: #{index}"
      client.indices.delete index: index
    rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
      p "[WARN] deleting index: #{index} failed. it does not exist: #{e}"
    end
  end

  desc 'delete_indices', 'deletes multiple es indices'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :indices,
                aliases: '-i',
                desc: 'the index to delete'

  method_option :index_prefix,
                aliases: '-r',
                desc: 'prefix for all indices to be deleted'

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def delete_indices(host = nil, port = nil, indices = nil, index_prefix = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    indices = options[:indices] if indices.nil?
    index_prefix = options[:index_prefix] if index_prefix.nil?
    begin
      verbose = options[:verbose] if verbose.nil?
    rescue StandardError
      FalseClass
    end
    if index_prefix.include? "\*"
      raise "not safe to use '*' in a index delete...use something to generate a list"
    elsif indices.include? "\*"
      raise "not safe to use '*' in a index delete...use something to generate a list"
    else
      indices = indices.split(',')
      indices.each do |index|
        delete_index(host, port, index, index_prefix, verbose)
      end
    end
  end

  desc 'reindex', 'Reindex a single index'
  method_option :source_host,
                aliases: '-h',
                desc: 'the host you wish to index from',
                required: true

  method_option :destination_host,
                aliases: '-H',
                desc: 'The destination to reindex into, can be the same cluster. Indices must exist prior to this being run',
                default: options[:source_host].default

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :source_index,
                aliases: '-i',
                desc: 'The name of the source index that will be reindexing, needs to exist',
                required: true

  method_option :destination_index,
                aliases: '-I',
                desc: 'The name of the destination index that will be reindexed into, needs to exist',
                required: true

  method_option :indices_prefix,
                desc: 'The prefix to use when reindexing',
                default: nil

  method_option :verbose,
                aliases: '-v',
                desc: 'Enable verbose logging',
                type: :boolean,
                default: true

  def reindex(source_host = nil, destination_host = nil, port = nil, source_index = nil, destination_index = nil, indices_prefix = nil, verbose = nil)
    source_host = options[:source_host] if source_host.nil?
    destination_host = options[:destination_host] if destination_host.nil?
    port = options[:port] if port.nil?
    source_index = options[:source_index] if source_index.nil?
    destination_index = options[:destination_index] if destination_index.nil?
    indices_prefix = options[:indices_prefix] if indices_prefix.nil?
    verbose = options[:verbose] if verbose.nil?

    # as the default client persists a connection for reindex we do something
    # special to work around faraday grabage: https://github.com/elastic/elasticsearch-ruby/tree/5.x/elasticsearch-transport#elasticsearchtransport
    require 'elasticsearch/transport'
    require 'typhoeus'
    require 'typhoeus/adapters/faraday'

    source_client = Elasticsearch::Client.new(
      host: source_host,
      port: port,
      log: verbose
    )

    unless source_client.indices.exists? index: "#{indices_prefix}#{source_index}"
      raise "LOG_LEVEL=ERROR [timestamp=#{Time.now}] [index=#{indices_prefix}#{source_index}] [message=source index does not exist, aborting!]"
    end

    dest_client = Elasticsearch::Client.new(
      host: destination_host,
      port: port,
      log: verbose
    )

    unless dest_client.indices.exists? index: "#{indices_prefix}#{destination_index}"
      raise "LOG_LEVEL=ERROR [timestamp=#{Time.now}] [index=#{destination_index}] [message=destination index does not exist, aborting!]"
    end

    p "starting reindex on: #{indices_prefix}#{source_index}"
    dest_client.reindex(
      timeout: '25m',
      body: {
        source: {
          remote: {
            host: "http://#{source_host}:#{port}"
          },
          index: "#{indices_prefix}#{source_index}"
        },
        dest: {
          index: "#{indices_prefix}#{destination_index}"
        }
      }
    )

    p "end of reindex: #{indices_prefix}#{destination_index}"
  end

  desc 'reindex_multiple', 'Reindex multiple indices, 1:1 relationship'
  method_option :source_host,
                aliases: '-h',
                desc: 'the host you wish to index from',
                required: true

  method_option :destination_host,
                aliases: '-H',
                desc: 'The destination to reindex into, can be the same cluster. Indices must exist prior to this being run',
                default: options[:source_host].default

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :source_indices,
                aliases: '-i',
                desc: 'A comma seperated list of indices to reindex, the destination indices need to exist',
                required: true

  method_option :destination_indices,
                aliases: '-I',
                desc: 'A comma seperated list of destination indices that will be reindexed into, needs to exist',
                required: true

  method_option :indices_prefix,
                desc: 'The prefix to use when reindexing',
                default: nil

  method_option :verbose,
                aliases: '-v',
                desc: 'Enable verbose logging',
                type: :boolean,
                default: true

  def reindex_multiple(source_host = nil, destination_host = nil, port = nil, source_indices = nil, destination_indices = nil, verbose = nil)
    source_host = options[:source_host] if source_host.nil?
    destination_host = options[:destination_host] if destination_host.nil?
    port = options[:port] if port.nil?
    source_indices = options[:source_indices] if source_indices.nil?
    destination_indices = options[:destination_indices] if destination_indices.nil?
    indices_prefix = options[:indices_prefix] if indices_prefix.nil?
    verbose = options[:verbose] if verbose.nil?

    src_indices = if source_indices.is_a?(Array)
                    source_indices
                  elsif source_indices.is_a(String)
                    source_indices.split(',')
                  else
                    raise TypeError, "source_indices: must be a string or array, you passed #{source_indices}"
                  end

    dest_indices = if destination_indices.is_a?(Array)
                     destination_indices
                   elsif destination_indices.is_a(String)
                     destination_indices.split(',')
                   else
                     raise TypeError, "destination_indices: must be a string or array, you passed #{destination_indices}"
                   end

    if src_indices.length != dest_indices.length
      raise "source: #{src_indices.length} dest: #{dest_indices.length} must match"
    end

    count = 0
    src_indices.each do |i|
      reindex(
        source_host,
        destination_host,
        port,
        src_indices[count],
        dest_indices[count],
        indices_prefix,
        verbose
      )
      count += 1
    end
  end

  desc 'migrate_indices_from_file', 'Takes a json file and populates the args to call migrate_indices with'
  method_option :file,
                aliases: '-f',
                desc: 'The path to the json file'

  method_option :indices_prefix,
                # aliases: '-f',
                desc: 'The prefix to the index name',
                default: nil

  method_option :source_host,
                aliases: '-h',
                desc: 'the source host you wish to migrate from'

  method_option :destination_host,
                aliases: '-H',
                desc: 'The destination to reindex into, can be the same cluster'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: true

  def migrate_indices_from_file(file = nil, source_host = nil, destination_host = nil, port = nil, indices_prefix = nil, verbose = true)
    file = options[:file] if file.nil?
    indices_prefix = options[:indices_prefix] if indices_prefix.nil?
    source_host = options[:source_host] if source_host.nil?
    destination_host = options[:destination_host] if destination_host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?

    unless File.exist?(file)
      raise ArgumentError, "file: #{file} does not exist"
    end

    begin
      data = JSON.parse(File.read(file))
    rescue TypeError => e
      raise TypeError, "we could not implicitly convert to string check for nil values: #{e}"
    rescue JSON::ParserError => e
      raise IOError, "we could not parse, is it valid json? #{e}"
    end

    indices = []
    shard_count = []

    data.each do |tenant_id, data_policy|
      indices << tenant_id
      shard_count << if data_policy['largeDataVolume'] == true
                       16
                     elsif data_policy['xlargeDataVolume'] == true
                       32
                     else
                       8
                     end
    end

    p indices

    migrate_indices(
      source_host,
      destination_host,
      port,
      indices,
      indices,
      shard_count,
      indices_prefix,
      verbose
    )
  end

  desc 'migrate_indices', 'Migrates indices across clusters, can be used in the same cluster as well. This includes creating indices and running a reindex for each one'
  method_option :source_host,
                aliases: '-h',
                desc: 'the source host you wish to migrate from'

  method_option :destination_host,
                aliases: '-H',
                desc: 'The destination to reindex into, can be the same cluster'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :source_indices,
                aliases: '-i',
                desc: 'a comma seperated list of indices to to migrate',
                required: true

  method_option :destination_indices,
                aliases: '-I',
                desc: 'a comma seperate list of indices to migrate',
                required: false

  method_option :shard_counts,
                aliases: '-s',
                desc: 'a comma seperated list of shards per index to create',
                required: true

  method_option :indices_prefix,
                desc: 'Indices prefix'

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: true

  def migrate_indices(source_host = nil, destination_host = nil, port = 9200, source_indices = nil, destination_indices = nil, shard_counts = nil, indices_prefix = nil, verbose = true)
    source_host = options[:source_host] if source_host.nil?
    destination_host = options[:destination_host] if destination_host.nil?
    port = options[:port] if port.nil?
    source_indices = options[:source_indices] if source_indices.nil?
    destination_indices = options[:destination_indices] if destination_indices.nil?
    shard_counts = options[:shard_counts] if shard_counts.nil?
    indices_prefix = options[:indices_prefix] if indices_prefix.nil?
    verbose = options[:verbose] if verbose.nil?

    src_indices =  if source_indices.is_a?(Array)
                     source_indices
                   elsif source_indices.is_a?(String)
                     source_indices.split(',')
                   else
                     raise TypeError, "source_indices: must be a string or array, you passed #{source_indices}"
                   end

    dest_indices = if destination_indices.nil?
                     # if none are specified we assume that the source indices
                     # names should be the same as the destination names
                     src_indices
                   elsif destination_indices.is_a?(Array)
                     destination_indices
                   elsif destination_indices.is_a?(String)
                     destination_indices.split(',')
                   else
                     raise TypeError, "destination_indices: must be a string, array, or nil (which defaults to source); you passed #{destination_indices}"
                   end

    # dest_indices = destination_indices.split(',')

    # TODO: should we support reindexing from multiple to single indices (thinking out time based indices)
    if src_indices.length != dest_indices.length
      raise "source: #{src_indices.length} dest: #{dest_indices.length} must match"
    end

    create_indices(
      destination_host,
      port,
      dest_indices,
      # TODO: will re-assses if makes sesnse to add an option to expose prefix at this level it just depends on the data we get
      indices_prefix,
      shard_counts,
      # enable logging
      verbose
    )

    reindex_multiple(
      source_host,
      destination_host,
      port,
      src_indices,
      dest_indices,
      true
    )
  end

  desc 'rolling_restart', 'does a rolling restart'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes'

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :user,
                aliases: '-u',
                desc: 'username to ssh as',
                default: `whoami`.chomp

  method_option :key,
                aliases: '-k', desc: 'key',
                default: ('/home/' + options[:user].default + '/.ssh/id_rsa')

  method_option :converge,
                aliases: '-e',
                desc: 'converge node default/false',
                type: :boolean,
                default: false

  method_option :include_client_nodes,
                aliases: '-c',
                desc: 'option to include client nodes in rolling restart - default: false',
                type: :boolean,
                default: true

  method_option :retries,
                aliases: '-r',
                desc: 'number of times to retry failures',
                default: nil

  method_option :sleep_time,
                aliases: '-s',
                desc: 'change how long we sleep - default: 10',
                default: 10,
                type: :numeric

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false

  def rolling_restart(host = nil, port = nil, user = nil, key = nil, converge = nil, include_client_nodes = nil, sleep_time = nil, retries = nil, _verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    user = options[:user] if user.nil?
    key = options[:key] if key.nil?
    converge = options[:converge] if converge.nil?
    include_client_nodes = options[:include_client_nodes] if include_client_nodes.nil?
    retries = options[:retries] if retries.nil?
    sleep_time = options[:sleep_time] if sleep_time.nil?
    # verbose = options[:verbose] if verbose.nil?
    wait_es_responds(host, port)
    role = 'current_master'
    current_master = get_nodes(host, port, role)
    # getting client and secondary master nodes
    role = 'master,data'
    nodes = get_nodes(host, port, role)
    # getting client nodes
    role = 'client'
    client_nodes = get_nodes(host, port, role)
    # for each node other than the current master (he should be done last)
    nodes.each do |node|
      wait_for_assigned_shards(host, port)
      wait_for_relocating_shards(host, port)
      cluster_shard_allocation(host, port, 'none', retries)
      restart_es_service(node, user, key, converge)
      sleep(sleep_time)
      wait_es_responds(host, port)
      cluster_shard_allocation(host, port, 'all')
      sleep(sleep_time)
      assign_unallocated_shards(host, port)
      wait_for_relocating_shards(host, port)
      wait_for_assigned_shards(host, port)
      sleep(sleep_time) # giving time for node to come up and join the cluster before we bring down another node
    end
    # do the client nodes if include_client_nodes flag is true
    if include_client_nodes
      client_nodes.each do |node|
        wait_es_responds(host, port)
        wait_for_relocating_shards(host, port)
        restart_es_service(node, user, key, converge)
        wait_es_responds(host, port)
        sleep(10) # giving time for node to come up and join the cluster before we bring down another node
      end
    end
    # do the current_master
    current_master.each do |node|
      wait_es_responds(host, port)
      wait_for_relocating_shards(host, port)
      restart_es_service(node, user, key, converge)
      wait_es_responds(host, port)
    end
  end

  desc 'exclude_node', 'exclude node from cluster'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :ip,
                aliases: '-a',
                desc: 'ipaddress of the node to be removed from cluster',
                required: true

  method_option :sleep_time,
                aliases: '-s',
                desc: 'change how long we sleep - default: 10',
                default: 10,
                type: :numeric

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false
  def exclude_node(host = nil, port = nil, ip = nil, verbose = nil, sleep_time = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    ip = options[:ip] if ip.nil?
    verbose = options[:verbose] if verbose.nil?
    sleep_time = options[:sleep_time] if sleep_time.nil?

    # wait_es_responds(host, port)
    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )

    cluster = client.cluster
    cluster.put_settings body: {
      transient: {
        "cluster.routing.allocation.exclude._ip": ip.to_s
      }
    }

    puts JSON.pretty_generate(cluster.get_settings)

    nodes = client.nodes
    size_in_bytes = 0
    begin
      index_metric = nodes.stats(node_id: ip, metric: 'indices', index_metric: 'store')
      size_in_bytes = index_metric['nodes'].values[0]['indices']['store']['size_in_bytes'].to_i
      puts "indices.store.size_in_bytes: #{size_in_bytes}"
      sleep sleep_time
    end while size_in_bytes > 0

    puts "Data has been moved completely off node #{ip}. It's now safe to remove #{ip} from cluster!"
  end

  desc 'reset_exclude', 'clear list of excluded ip addresses of cluster'
  method_option :host,
                aliases: '-h',
                desc: 'the host to do this from, you should probably use the current_master which can be obtained from get_nodes',
                required: true

  method_option :port,
                aliases: '-p',
                desc: 'the port es runs on',
                default: 9200

  method_option :verbose,
                aliases: '-v',
                desc: 'verbose mode, turns on es logging',
                type: :boolean,
                default: false
  def reset_exclude(host = nil, port = nil, verbose = nil)
    host = options[:host] if host.nil?
    port = options[:port] if port.nil?
    verbose = options[:verbose] if verbose.nil?

    client = Elasticsearch::Client.new(
      host: host,
      port: port,
      log: verbose
    )

    cluster = client.cluster
    puts JSON.pretty_generate(cluster.get_settings)
    cluster.put_settings body: {
      transient: {
        "cluster.routing.allocation.exclude._ip": ''
      }
    }

    puts JSON.pretty_generate(cluster.get_settings)
    puts 'Cluster excluded list has been cleared!'
  end

  # end of class
end
ESUtils.start(ARGV)
