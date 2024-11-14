# Copyright (c) 2020-2024 Status Research & Development GmbH
# Licensed under either of
#  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
#  * MIT license ([LICENSE-MIT](LICENSE-MIT))
# at your option.
# This file may not be copied, modified, or distributed except according to
# those terms.

{.push raises: [].}

import
  std/[options, strutils, tables, sets, math],
  confutils, confutils/std/net, chronicles, chronicles/topics_registry,
  chronos, metrics, metrics/chronos_httpserver, stew/byteutils, stew/bitops2,
  eth/keys, eth/net/nat,
  eth/p2p/discoveryv5/[enr, node, routing_table],
  eth/p2p/discoveryv5/protocol as discv5_protocol,
  clickhouse_client

const
  defaultListenAddress* = (static parseIpAddress("0.0.0.0"))
  defaultAdminListenAddress* = (static parseIpAddress("127.0.0.1"))
  defaultListenAddressDesc = $defaultListenAddress
  defaultAdminListenAddressDesc = $defaultAdminListenAddress

let dcrawlStartTime = chronos.Moment.now
logScope:
  topics = "dcrawl"
  ts2 = (chronos.Moment.now - dcrawlStartTime).milliseconds

type
  OutputType = enum
    otCsv, otClickHouse, otBoth

  DiscoveryCmd* = enum
    noCommand
    ping
    findNode
    talkReq

  DiscoveryConf* = object
    logLevel* {.
      defaultValue: LogLevel.DEBUG
      desc: "Sets the log level"
      name: "log-level" .}: LogLevel

    udpPort* {.
      defaultValue: 9009
      desc: "UDP listening port"
      name: "udp-port" .}: uint16

    listenAddress* {.
      defaultValue: defaultListenAddress
      defaultValueDesc: $defaultListenAddressDesc
      desc: "Listening address for the Discovery v5 traffic"
      name: "listen-address" }: IpAddress

    persistingFile* {.
      defaultValue: "peerstore.csv",
      desc: "File where the tool will keep the measured records"
      name: "persisting-file" .}: string

    discoveryFile* {.
      defaultValue: "discovery.csv",
      desc: "File where the tool will keep the discovered records"
      name: "discovery-file" .}: string

    outputType* {.
      defaultValue: "csv"
      desc: "Output type (csv, clickhouse, or both)"
      name: "output-type" .}: string

    clickhouseHost* {.
      defaultValue: ""
      desc: "ClickHouse server host"
      name: "clickhouse-host" .}: string

    clickhousePort* {.
      defaultValue: 8443
      desc: "ClickHouse server port"
      name: "clickhouse-port" .}: int

    clickhouseDb* {.
      defaultValue: "default"
      desc: "ClickHouse database name"
      name: "clickhouse-db" .}: string

    clickhouseTable* {.
      defaultValue: "ethereum_nodes"
      desc: "ClickHouse table name"
      name: "clickhouse-table" .}: string

    clickhouseUser* {.
      defaultValue: ""
      desc: "ClickHouse username"
      name: "clickhouse-user" .}: string

    clickhousePassword* {.
      defaultValue: ""
      desc: "ClickHouse password"
      name: "clickhouse-password" .}: string

    clickhouseSecure* {.
      defaultValue: true
      desc: "Use HTTPS for ClickHouse connection"
      name: "clickhouse-secure" .}: bool

    bootnodes* {.
      desc: "ENR URI of node to bootstrap discovery with. Argument may be repeated"
      name: "bootnode" .}: seq[enr.Record]

    nat* {.
      desc: "Specify method to use for determining public address. " &
            "Must be one of: any, none, upnp, pmp, extip:<IP>"
      defaultValue: NatConfig(hasExtIp: false, nat: NatAny)
      name: "nat" .}: NatConfig

    enrAutoUpdate* {.
      defaultValue: false
      desc: "Discovery can automatically update its ENR with the IP address " &
            "and UDP port as seen by other nodes it communicates with. " &
            "This option allows to enable/disable this functionality"
      name: "enr-auto-update" .}: bool

    nodeKey* {.
      desc: "P2P node private key as hex",
      defaultValue: PrivateKey.random(keys.newRng()[])
      name: "nodekey" .}: PrivateKey

    queryIntervalUs* {.
      desc: "interval between findNode queries in microsecond",
      defaultValue: 1000
      name: "query-interval-us" .}: int

    fullCycles* {.
      desc: "full measurement cycles to run",
      defaultValue: 1
      name: "cycles" .}: int

    metricsEnabled* {.
      defaultValue: false
      desc: "Enable the metrics server"
      name: "metrics" .}: bool

    metricsAddress* {.
      defaultValue: defaultAdminListenAddress
      defaultValueDesc: $defaultAdminListenAddressDesc
      desc: "Listening address of the metrics server"
      name: "metrics-address" .}: IpAddress

    metricsPort* {.
      defaultValue: 8008
      desc: "Listening HTTP port of the metrics server"
      name: "metrics-port" .}: Port

proc initClickHouseTable(config: DiscoveryConf) {.raises: [].} =
  if config.outputType notin ["clickhouse", "both"]:
    return

  try:
    let client = newClickHouseClient(
      host = config.clickhouseHost,
      port = config.clickhousePort,
      database = config.clickhouseDb,
      username = config.clickhouseUser,
      password = config.clickhousePassword,
      secure = config.clickhouseSecure
    )

    # Create tables if they don't exist
    const createPeerstoreTable = """
      CREATE TABLE IF NOT EXISTS $1 (
        cycle Int32,
        node_id String,
        address String,
        rtt_min Int32,
        rtt_avg Int32,
        bw_max_mbps Float64,
        bw_avg_mbps Float64,
        pubkey String,
        fork_digest String,
        attnets String,
        attnets_number Int32,
        client String,
        enr String,
        timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (timestamp, node_id)
    """

    const createDiscoveryTable = """
      CREATE TABLE IF NOT EXISTS $1_discovery (
        cycle Int32,
        node_id String,
        address String,
        pubkey String,
        fork_digest String,
        attnets String,
        attnets_number Int32,
        client String,
        enr String,
        timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (timestamp, node_id)
    """

    discard client.execute(createPeerstoreTable % [config.clickhouseTable])
    discard client.execute(createDiscoveryTable % [config.clickhouseTable])
    info "ClickHouse tables initialized"
    client.close()
  except Exception as e:
    error "Failed to initialize ClickHouse tables", msg = e.msg


proc writeToCH(config: DiscoveryConf, tableName: string, values: seq[string]) =
  if config.outputType notin ["clickhouse", "both"]:
    return

  let client = newClickHouseClient(
    host = config.clickhouseHost,
    port = config.clickhousePort,
    database = config.clickhouseDb,
    username = config.clickhouseUser,
    password = config.clickhousePassword,
    secure = config.clickhouseSecure
  )

  let query = "INSERT INTO " & tableName & " VALUES (" & values.join(",") & ")"
  discard client.execute(query)

proc parseCmdArg*(T: type enr.Record, p: string): T {.raises: [ValueError].} =
  let res = enr.Record.fromURI(p)
  if res.isErr:
    raise newException(ValueError, "Invalid ENR:" & $res.error)
  res.value

proc completeCmdArg*(T: type enr.Record, val: string): seq[string] =
  return @[]

proc parseCmdArg*(T: type Node, p: string): T {.raises: [ValueError].} =
  let res = enr.Record.fromURI(p)
  if res.isErr:
    raise newException(ValueError, "Invalid ENR:" & $res.error)

  let n = Node.fromRecord(res.value)
  if n.address.isNone():
    raise newException(ValueError, "ENR without address")
  n

proc completeCmdArg*(T: type Node, val: string): seq[string] =
  return @[]

proc parseCmdArg*(T: type PrivateKey, p: string): T {.raises: [ValueError].} =
  try:
    result = PrivateKey.fromHex(p).tryGet()
  except CatchableError:
    raise newException(ValueError, "Invalid private key")

proc completeCmdArg*(T: type PrivateKey, val: string): seq[string] =
  return @[]

func toString*(bytes: openArray[byte]): string {.inline.} =
  let length = bytes.len
  if length > 0:
    result = newString(length)
    copyMem(result.cstring, bytes[0].unsafeAddr, length)

proc ethDataExtract(dNode: Node): auto =
  let pubkey = dNode.record.tryGet("secp256k1", seq[byte])
  let eth2 = dNode.record.tryGet("eth2", seq[byte])
  let attnets = dNode.record.tryGet("attnets", seq[byte])
  let client = dNode.record.tryGet("client", seq[byte])

  var bits = 0
  if not attnets.isNone:
    for byt in attnets.get():
      bits.inc(countOnes(byt.uint))

  (pubkey, eth2, attnets, bits, client)

proc discover(d: discv5_protocol.Protocol, interval: Duration, fullCycles: int,
              config: DiscoveryConf) {.async: (raises: [CancelledError]).} =

  var queuedNodes: HashSet[Node] = d.randomNodes(int.high).toHashSet
  var measuredNodes: HashSet[Node]
  var failedNodes: HashSet[Node]
  var pendingQueries: seq[Future[void]]
  var discoveredNodes: HashSet[Node]
  var enrsReceived: int64
  var cycle = 0

  if config.outputType in ["clickhouse", "both"]:
    initClickHouseTable(config)

  info "Starting peer-discovery in Ethereum"

  let ps =
    if config.outputType in ["csv", "both"]:
      try:
        open(config.persistingFile, fmWrite)
      except IOError as e:
        fatal "Failed to open file for writing", file = config.persistingFile, error = e.msg
        quit QuitFailure
    else:
      nil

  let ds =
    if config.outputType in ["csv", "both"]:
      try:
        open(config.discoveryFile, fmWrite)
      except IOError as e:
        fatal "Failed to open file for writing", file = config.discoveryFile, error = e.msg
        quit QuitFailure
    else:
      nil

  if ps != nil:
    defer: ps.close()
    try:
      ps.writeLine("cycle,node_id,ip:port,rttMin,rttAvg,bwMaxMbps,bwAvgMbps,pubkey,forkDigest,attnets,attnets_number,client,enr")
    except IOError as e:
      fatal "Failed to write to file", file = config.persistingFile, error = e.msg
      quit QuitFailure

  if ds != nil:
    defer: ds.close()
    try:
      ds.writeLine("cycle,node_id,ip:port,pubkey,forkDigest,attnets,attnets_number,client,enr")
    except IOError as e:
      fatal "Failed to write to file", file = config.discoveryFile, error = e.msg
      quit QuitFailure

  proc measureOne(n: Node, distances: seq[uint16]) {.async: (raises: [CancelledError]).} =
    let iTime = now(chronos.Moment)
    let find = await d.findNode(n, distances)
    let qDuration = now(chronos.Moment) - iTime
    if find.isOk():
      let discovered = find[]
      enrsReceived += discovered.len

      var queuedNew = 0
      for dNode in discovered:
        if not discoveredNodes.contains(dNode):
          queuedNodes.incl(dNode)
          queuedNew += 1
          discoveredNodes.incl(dNode)
          debug "discoveredNew", id=dNode.id.toHex, addr=dNode.address.get(), enrv=dNode.record.seqNum
          
          let (pubkey, eth2, attnets, bits, client) = ethDataExtract(dNode)
          let discoveryValues = @[
            $cycle,
            "'" & dNode.id.toHex & "'",
            "'" & $dNode.address.get() & "'",
            "'" & pubkey.get(@[]).toHex & "'",
            "'" & eth2.get(@[0'u8,0,0,0])[0..3].toHex & "'",
            "'" & attnets.get(@[]).toHex & "'",
            $bits,
            "'" & client.get(@[]).toString & "'",
            "'" & $dNode.record & "'"
          ]

          if ds != nil:
            try:
              ds.writeLine(discoveryValues.join(","))
            except IOError as e:
              fatal "Failed to write to file", file = config.discoveryFile, error = e.msg
              quit QuitFailure

          if config.outputType in ["clickhouse", "both"]:
            writeToCH(config, config.clickhouseTable & "_discovery", discoveryValues)

        else:
          debug "discoveredOld", id=dNode.id.toHex, addr=dNode.address.get(), enrv=dNode.record.seqNum

      debug "findNode finished", id=n.id.toHex, query_time = qDuration.milliseconds,
        received = discovered.len, new = queuedNew,
        queued = queuedNodes.len, measured = measuredNodes.len, failed = failedNodes.len,
        rtlen = d.routingTable.len,
        pending = pendingQueries.len,
        discovered = discoveredNodes.len,
        receivedSum = enrsReceived,
        distances

      let
        rttMin = n.stats.rttMin.int
        rttAvg = n.stats.rttAvg.int
        bwMaxMbps = (n.stats.bwMax / 1e6).round(3)
        bwAvgMbps = (n.stats.bwAvg / 1e6).round(3)

      trace "crawl", n, rttMin, rttAvg, bwMaxMbps, bwAvgMbps
      measuredNodes.incl(n)

      let (pubkey, eth2, attnets, bits, client) = ethDataExtract(n)
      let peerstoreValues = @[
        $cycle,
        "'" & n.id.toHex & "'",
        "'" & $n.address.get() & "'",
        $rttMin,
        $rttAvg,
        $bwMaxMbps,
        $bwAvgMbps,
        "'" & pubkey.get(@[]).toHex & "'",
        "'" & eth2.get(@[0'u8,0,0,0])[0..3].toHex & "'",
        "'" & attnets.get(@[]).toHex & "'",
        $bits,
        "'" & client.get(@[]).toString & "'",
        "'" & $n.record & "'"
      ]

      if ps != nil:
        try:
          ps.writeLine(peerstoreValues.join(","))
        except IOError as e:
          fatal "Failed to write to file", file = config.persistingFile, error = e.msg
          quit QuitFailure

      if config.outputType in ["clickhouse", "both"]:
        writeToCH(config, config.clickhouseTable, peerstoreValues)

    else:
      failedNodes.incl(n)
      debug "findNode failed", id=n.id.toHex, addr=n.address.get(), query_time = qDuration.milliseconds, enrv=n.record.seqNum

  proc measureAwaitOne(n: Node, distances: seq[uint16]) {.async: (raises: [CancelledError]).} =
    let f = measureOne(n, distances)
    pendingQueries.add(f)

    await f

    let index = pendingQueries.find(f)
    if index != -1:
      pendingQueries.del(index)
    else:
      error "Resulting query should have been in the pending queries"

  while cycle < fullCycles:
    var d = 0
    let drange = 16
    block:
      for retry in 0 .. 0:
        while true:
          try:
            let n = queuedNodes.pop()
            trace "measuring", n, dist=256-d
            discard measureAwaitOne(n, @[(256-d).uint16])
            d = (d+1) mod drange

            await sleepAsync(interval)
          except KeyError:
            if pendingQueries.len > 0:
              debug "pending queries, waiting"
              await sleepAsync(1.milliseconds)
            else:
              queuedNodes = failedNodes
              failedNodes.clear
              break
      queuedNodes = measuredNodes
      failedNodes.clear
    info "no more nodes in cycle, starting next cycle", cycle, measured = measuredNodes.len, failed = failedNodes.len
    cycle += 1

proc validateConfig(config: DiscoveryConf): bool =
  if config.outputType notin ["csv", "clickhouse", "both"]:
    error "Invalid output type", outputType=config.outputType
    return false
    
  if config.outputType in ["clickhouse", "both"]:
    if config.clickhouseHost.len == 0:
      error "ClickHouse host not configured"
      return false
    if config.clickhouseUser.len == 0:
      error "ClickHouse user not configured"
      return false
    if config.clickhousePassword.len == 0:
      error "ClickHouse password not configured"
      return false
      
  return true

proc run(config: DiscoveryConf) {.raises: [CatchableError].} =
  if not validateConfig(config):
    quit(1)

  let
    bindIp = config.listenAddress
    udpPort = Port(config.udpPort)
    (extIp, _, extUdpPort) = setupAddress(config.nat,
      config.listenAddress, udpPort, udpPort, "dcli")

  let d = newProtocol(config.nodeKey,
          extIp, Opt.none(Port), extUdpPort,
          bootstrapRecords = config.bootnodes,
          bindIp = bindIp, bindPort = udpPort,
          enrAutoUpdate = config.enrAutoUpdate)

  d.open()

  if config.metricsEnabled:
    let
      address = config.metricsAddress
      port = config.metricsPort
      url = "http://" & $address & ":" & $port & "/metrics"
      server = MetricsHttpServerRef.new($address, port).valueOr:
        error "Could not instantiate metrics HTTP server", url, error
        quit QuitFailure

    info "Starting metrics HTTP server", url
    try:
      waitFor server.start()
    except MetricsError as exc:
      fatal "Could not start metrics HTTP server",
        url, error_msg = exc.msg, error_name = exc.name
      quit QuitFailure

  waitFor(discover(d, config.queryIntervalUs.microseconds,
                   config.fullCycles, config))

when isMainModule:
  {.pop.}
  let config = DiscoveryConf.load()
  {.push raises: [].}

  setLogLevel(config.logLevel)

  run(config)