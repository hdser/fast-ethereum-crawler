import std/[os, strutils, times]
import chronicles
import clickhouse_client
import options

type
  OutputConfig* = object
    outputType*: string  # "csv", "clickhouse", or "both"
    csvPath*: string
    clickhouseHost*: string
    clickhousePort*: int
    clickhouseDatabase*: string
    clickhouseTable*: string
    clickhouseUser*: string
    clickhousePassword*: string
    clickhouseSecure*: bool

  NodeData* = object
    timestamp*: Time
    nodeId*: string
    ip*: string
    tcpPort*: int
    udpPort*: int
    lastSeen*: Time
    distance*: int
    
proc newOutputConfig*(): OutputConfig =
  result = OutputConfig(
    outputType: getEnv("OUTPUT_TYPE", "csv"),
    csvPath: getEnv("CSV_PATH", "results"),
    clickhouseHost: getEnv("CLICKHOUSE_HOST", ""),
    clickhousePort: parseInt(getEnv("CLICKHOUSE_PORT", "8443")),
    clickhouseDatabase: getEnv("CLICKHOUSE_DB", "default"),
    clickhouseTable: getEnv("CLICKHOUSE_TABLE", "ethereum_nodes"),
    clickhouseUser: getEnv("CLICKHOUSE_USER", ""),
    clickhousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
    clickhouseSecure: parseBool(getEnv("CLICKHOUSE_SECURE", "true"))
  )

proc validateConfig*(config: OutputConfig): bool =
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

proc initClickHouseTable*(config: OutputConfig) =
  if config.outputType notin ["clickhouse", "both"]:
    return
    
  let client = newClickHouseClient(
    host = config.clickhouseHost,
    port = config.clickhousePort,
    database = config.clickhouseDatabase,
    username = config.clickhouseUser,
    password = config.clickhousePassword,
    secure = config.clickhouseSecure
  )
  
  # Create table if not exists
  const createTableQuery = """
    CREATE TABLE IF NOT EXISTS $1 (
      timestamp DateTime,
      node_id String,
      ip String,
      tcp_port UInt16,
      udp_port UInt16,
      last_seen DateTime,
      distance UInt8
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, node_id)
  """
  
  discard client.execute(createTableQuery % [config.clickhouseTable])
  info "ClickHouse table initialized", table=config.clickhouseTable

proc writeToClickHouse*(config: OutputConfig, data: NodeData) =
  if config.outputType notin ["clickhouse", "both"]:
    return
    
  let client = newClickHouseClient(
    host = config.clickhouseHost,
    port = config.clickhousePort,
    database = config.clickhouseDatabase,
    username = config.clickhouseUser,
    password = config.clickhousePassword,
    secure = config.clickhouseSecure
  )
  
  const insertQuery = """
    INSERT INTO $1 (
      timestamp, node_id, ip, tcp_port, udp_port, last_seen, distance
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?
    )
  """
  
  let params = @[
    data.timestamp.toTime().toUnix(),
    data.nodeId,
    data.ip,
    data.tcpPort,
    data.udpPort,
    data.lastSeen.toTime().toUnix(),
    data.distance
  ]
  
  discard client.execute(insertQuery % [config.clickhouseTable], params)
  debug "Data written to ClickHouse", nodeId=data.nodeId

proc writeToCsv*(config: OutputConfig, data: NodeData) =
  if config.outputType notin ["csv", "both"]:
    return
    
  let csvLine = [
    $data.timestamp.toTime().toUnix(),
    data.nodeId,
    data.ip,
    $data.tcpPort,
    $data.udpPort,
    $data.lastSeen.toTime().toUnix(),
    $data.distance
  ].join(",")
  
  let filename = config.csvPath / "nodes.csv"
  var f = open(filename, fmAppend)
  f.writeLine(csvLine)
  f.close()
  
  debug "Data written to CSV", nodeId=data.nodeId

proc writeNodeData*(config: OutputConfig, data: NodeData) =
  case config.outputType:
    of "csv": writeToCsv(config, data)
    of "clickhouse": writeToClickHouse(config, data)
    of "both":
      writeToCsv(config, data)
      writeToClickHouse(config, data)
    else: error "Invalid output type", outputType=config.outputType