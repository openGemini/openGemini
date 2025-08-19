# openGemini MCP Server

A Model Context Protocol (MCP) server implementation for openGemini.

This server provides AI assistants with a secure and structured way to explore and analyze databases.

## Capabilities

* `list_databases`
  - List all databases on your openGemini cluster.

* `list_measurements`
  - List all measurements in a database.
  - Input: `database` (string): The name of the database.

* `read_resource`
  - Read sample data from a measurement.
  - Input: `measurement` (string): The name of the measurement.

* `execute_influxql`
  - Execute an InfluxQL on your openGemini cluster.
  - Input: `query` (string): InfluxQL query to execute.
  - Only support query start with `SELECT` or `SHOW`.

## Installation
```bash
pip install . 
```

## Configuration

1. Open the Claude Desktop configuration file located at:
   - On macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

2. Add the following:

```json
{
  "mcpServers": {
    "mcp-openGemini": {
      "command": "/path/to/python",
      "args": [
        "-m",
        "mcp_openGemini.server"
      ],
      "env": {
        "OPENGEMINI_HOST": "<opengemini-host>",
        "OPENGEMINI_PORT": "<opengemini-port>",
        "OPENGEMINI_USER": "<opengemini-user>",
        "OPENGEMINI_PASSWORD": "<opengemini-password>"
      }
    }
  }
}
```

Update the environment variables to point to your own openGemini service.

3. Restart Claude Desktop to apply the changes.