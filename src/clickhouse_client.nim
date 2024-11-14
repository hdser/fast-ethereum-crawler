import std/[httpclient, strutils, base64, uri]
import chronicles

type
  ClickHouseClient* = ref object
    host*: string
    port*: int
    database*: string
    username*: string
    password*: string
    secure*: bool
    client: HttpClient

  ClickHouseError* = object of CatchableError

proc newClickHouseClient*(host: string, port: int, database: string, 
                         username: string, password: string, 
                         secure: bool = true): ClickHouseClient {.raises: [].} =
  try:
    result = ClickHouseClient(
      host: host,
      port: port,
      database: database,
      username: username,
      password: password,
      secure: secure
    )
    result.client = newHttpClient()
    result.client.headers = newHttpHeaders({
      "Content-Type": "application/json",
      "Accept": "application/json"
    })
    # Basic auth
    let auth = encode($username & ":" & $password)
    result.client.headers.add("Authorization", "Basic " & auth)
  except Exception as e:
    error "Failed to create ClickHouse client", msg = e.msg

proc getUrl(client: ClickHouseClient, query: string): string {.raises: [].} =
  let protocol = if client.secure: "https" else: "http"
  try:
    result = protocol & "://" & client.host & ":" & $client.port & 
             "/?database=" & encodeUrl(client.database) & "&query=" & encodeUrl(query)
  except Exception as e:
    error "Failed to create URL", msg = e.msg
    result = ""

proc execute*(client: ClickHouseClient, query: string, 
              params: seq[string] = @[]): bool {.gcsafe, raises: [].} =
  ## Execute a query and return success status
  var finalQuery = query  # Moved declaration outside try block
  
  try:
    if params.len > 0:
      # Simple parameter substitution
      for param in params:
        # Replace only first occurrence of "?" with the parameter
        let idx = finalQuery.find("?")
        if idx >= 0:
          finalQuery = finalQuery[0 ..< idx] & param & finalQuery[idx + 1 .. ^1]
    
    let url = client.getUrl(finalQuery)
    if url.len == 0:
      return false

    let response = client.client.post(url, "")
    let statusCode = response.code.int
    
    if statusCode != 200:
      error "ClickHouse query failed", 
        statusCode = $statusCode, 
        responseBody = response.body,
        query = finalQuery
      return false
    return true
  except Exception as e:
    error "ClickHouse query error", 
      errorMsg = e.msg, 
      query = finalQuery
    return false

proc close*(client: ClickHouseClient) {.raises: [].} =
  try:
    if not client.client.isNil:
      client.client.close()
  except Exception as e:
    error "Failed to close client", msg = e.msg