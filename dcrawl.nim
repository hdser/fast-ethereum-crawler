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
  eth/p2p/discoveryv5/[enr, node],
  eth/p2p/discoveryv5/protocol as discv5_protocol

const
  defaultListenAddress* = (static parseIpAddress("0.0.0.0"))
  defaultAdminListenAddress* = (static parseIpAddress("127.0.0.1"))
  defaultListenAddressDesc = $defaultListenAddress
  defaultAdminListenAddressDesc = $defaultAdminListenAddress

type
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
      desc: "File where the tool will keep the discovered records"
      name: "persisting-file" .}: string

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

    case cmd* {.
      command
      defaultValue: noCommand }: DiscoveryCmd
    of noCommand:
      discard
    of ping:
      pingTarget* {.
        argument
        desc: "ENR URI of the node to a send ping message"
        name: "node" .}: Node
    of findNode:
      distance* {.
        defaultValue: 255
        desc: "Distance parameter for the findNode message"
        name: "distance" .}: uint16
      # TODO: Order here matters as else the help message does not show all the
      # information, see: https://github.com/status-im/nim-confutils/issues/15
      findNodeTarget* {.
        argument
        desc: "ENR URI of the node to send a findNode message"
        name: "node" .}: Node
    of talkReq:
      talkReqTarget* {.
        argument
        desc: "ENR URI of the node to send a talkReq message"
        name: "node" .}: Node

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

proc discover(d: discv5_protocol.Protocol, psFile: string) {.async: (raises: [CancelledError]).} =

  var queuedNodes: HashSet[Node] = d.randomNodes(int.high).toHashSet
  var measuredNodes: HashSet[Node]

  info "Starting peer-discovery in Ethereum - persisting peers at: ", psFile

  let ps =
    try:
      open(psFile, fmWrite)
    except IOError as e:
      fatal "Failed to open file for writing", file = psFile, error = e.msg
      quit QuitFailure
  defer: ps.close()
  try:
    ps.writeLine("ip:port,rttMin,rttAvg,bwMaxMbps,bwAvgMbps")
  except IOError as e:
    fatal "Failed to write to file", file = psFile, error = e.msg
    quit QuitFailure

  proc measureOne(n: Node) {.async: (raises: [CancelledError]).} =
    let iTime = now(chronos.Moment)
    let find = await d.findNode(n, @[256'u16, 255'u16, 254'u16])
    let qDuration = now(chronos.Moment) - iTime
    if find.isOk():
      let discovered = find[]
      debug "findNode finished",  query_time = qDuration.secs, new_nodes = discovered.len, tot_peers=len(queuedNodes)
      let
        rttMin = n.stats.rttMin.int
        rttAvg = n.stats.rttAvg.int
        bwMaxMbps = (n.stats.bwMax / 1e6).round(3)
        bwAvgMbps = (n.stats.bwAvg / 1e6).round(3)

      debug "crawl", n, rttMin, rttAvg, bwMaxMbps, bwAvgMbps
      measuredNodes.incl(n)

      try:
        let newLine = "$#,$#,$#,$#,$#" % [$n.address.get(), $rttMin, $rttAvg, $bwMaxMbps, $bwAvgMbps]
        ps.writeLine(newLine)
      except ValueError as e:
        raiseAssert e.msg
      except IOError as e:
        fatal "Failed to write to file", file = psFile, error = e.msg
        quit QuitFailure

      for dNode in discovered:
        if not measuredNodes.contains(dNode):
          queuedNodes.incl(dNode)

    else:
      debug "findNode failed"


  while true:
    try:
      let n = queuedNodes.pop()
      debug "measuring", n
      await measureOne(n)

      await sleepAsync(100.milliseconds) # 100 msec of delay
    except KeyError:
      info "no more nodes"
      break



proc run(config: DiscoveryConf) {.raises: [CatchableError].} =
  let
    bindIp = config.listenAddress
    udpPort = Port(config.udpPort)
    # TODO: allow for no TCP port mapping!
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

  case config.cmd
  of ping:
    let pong = waitFor d.ping(config.pingTarget)
    if pong.isOk():
      echo pong[]
    else:
      echo "No Pong message returned"
  of findNode:
    let nodes = waitFor d.findNode(config.findNodeTarget, @[config.distance])
    if nodes.isOk():
      echo "Received valid records:"
      for node in nodes[]:
        echo $node.record & " - " & shortLog(node)
    else:
      echo "No Nodes message returned"
  of talkReq:
    let talkresp = waitFor d.talkReq(config.talkReqTarget, @[], @[])
    if talkresp.isOk():
      echo talkresp[]
    else:
      echo "No Talk Response message returned"
  of noCommand:
    # do not call start, otherwise we start with two findnodes to the same node, which fails
    # d.start()
    waitFor(discover(d, config.persistingFile))

when isMainModule:
  {.pop.}
  let config = DiscoveryConf.load()
  {.push raises: [].}

  setLogLevel(config.logLevel)

  run(config)
