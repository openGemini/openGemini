from influxdb import InfluxDBClient
from mcp_openGemini.config import Config

import asyncio
import logging
from logging import Logger
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl

# Resource URI prefix
RES_PREFIX = "openGemini://"
# Resource query results limit
RESULTS_LIMIT = 100

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# The openGemini MCP Server
class openGeminiServer:
    def __init__(self, logger: Logger, config: Config):
        """Initialize the openGemini MCP server"""
        self.app = Server("openGemini_mcp_server")
        self.logger = logger
        self.db_config = {
            "host": config.host,
            "port": config.port,
            "username": config.user,
            "password": config.password,
            "database": config.database,
        }

        # Initialize openGemini client
        self.client = InfluxDBClient(
            host=self.db_config["host"],
            port=self.db_config["port"],
            username=self.db_config["username"],
            password=self.db_config["password"],
            database=self.db_config["database"],
        )

        # Register callbacks
        self.app.list_resources()(self.list_databases)
        self.app.list_resources()(self.list_measurements)
        self.app.read_resource()(self.read_resource)
        self.app.list_tools()(self.list_tools)
        self.app.call_tool()(self.call_tool)

    async def list_databases(self) -> list[Resource]:
        """List openGemini databases as resources."""
        logger = self.logger
        config = self.db_config

        try:
            query = "SHOW DATABASES"
            result = self.client.query(query)
            measurements = [row["name"] for row in result.get_points()]

            resources = []
            for measurement in measurements:
                resources.append(
                    Resource(
                        uri=f"{RES_PREFIX}{measurement}/data",
                        name=f"Database: {measurement}",
                        mimeType="text/plain",
                        description=f"Data in Database: {measurement}",
                    )
                )
            return resources
        except Exception as e:
            logger.error(f"Failed to list databases: {str(e)}")
            return []

    async def list_measurements(self) -> list[Resource]:
        """List openGemini measurements as resources."""
        logger = self.logger
        config = self.db_config

        try:
            query = "SHOW MEASUREMENTS"
            result = self.client.query(query)
            measurements = [row["name"] for row in result.get_points()]

            resources = []
            for measurement in measurements:
                resources.append(
                    Resource(
                        uri=f"{RES_PREFIX}{measurement}/data",
                        name=f"Measurement: {measurement}",
                        mimeType="text/plain",
                        description=f"Data in measurement: {measurement}",
                    )
                )
            return resources
        except Exception as e:
            logger.error(f"Failed to list measurements: {str(e)}")
            return []

    async def read_resource(self, uri: AnyUrl) -> str:
        """Read measurement data."""
        logger = self.logger
        config = self.db_config

        uri_str = str(uri)
        logger.info(f"Reading resource: {uri_str}")

        if not uri_str.startswith(RES_PREFIX):
            raise ValueError(f"Invalid URI scheme: {uri_str}")

        parts = uri_str[len(RES_PREFIX) :].split("/")
        measurement = parts[0]

        try:
            query = f'SELECT * FROM "{measurement}" LIMIT {RESULTS_LIMIT}'
            result = self.client.query(query)
            points = list(result.get_points())

            if not points:
                return "No data found"

            # Extract columns and rows
            columns = points[0].keys()
            rows = []
            for point in points:
                rows.append([str(point[col]) for col in columns])

            # Format as CSV
            result_csv = []
            if columns:
                result_csv.append(",".join(columns))
                for row in rows:
                    result_csv.append(",".join(row))
            return "\n".join(result_csv)
        except Exception as e:
            logger.error(f"Database error reading resource {uri}: {str(e)}")
            raise RuntimeError(f"Database error: {str(e)}")

    async def list_tools(self) -> list[Tool]:
        """List available openGemini tools."""
        logger = self.logger

        logger.info("Listing tools...")
        return [
            Tool(
                name="execute_influxql",
                description="Execute InfluxQL query against openGemini.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The InfluxQL query to execute",
                        }
                    },
                    "required": ["query"],
                },
            )
        ]

    async def call_tool(self, name: str, arguments: dict) -> list[TextContent]:
        """Execute InfluxQL commands."""
        logger = self.logger
        config = self.db_config

        logger.info(f"Calling tool: {name} with arguments: {arguments}")

        if name != "execute_influxql":
            raise ValueError(f"Unknown tool: {name}")

        query = arguments.get("query")

        if not query:
            raise ValueError("Query is required")

        query_upper = query.strip().upper()

        if not (query_upper.startswith("SELECT") or query_upper.startswith("SHOW")):
            logger.error("Only SELECT or SHOW queries are allowed")
            return [
                TextContent(type="text", text="Only SELECT or SHOW queries are allowed")
            ]

        if len([part for part in query.strip().split(';') if part.strip()]) > 1:
            logger.error("Multiple statements are not allowed")
            return [
                TextContent(type="text", text="Multiple statements are not allowed")
            ]

        if (
            "DROP" in query_upper
            or "ALTER" in query_upper
            or "INSERT" in query_upper
            or "CREATE" in query_upper
        ):
            logger.error("Dangerous operations are not allowed")
            return [
                TextContent(type="text", text="Dangerous operations are not allowed")
            ]

        try:
            result = self.client.query(query)
            points = list(result.get_points())

            if not points:
                return [TextContent(type="text", text="No results")]

            # Extract columns and rows
            columns = points[0].keys()
            rows = []
            for point in points:
                rows.append([str(point[col]) for col in columns])

            # Format as CSV
            result_csv = []
            if columns:
                result_csv.append(",".join(columns))
                for row in rows:
                    result_csv.append(",".join(row))

            return [TextContent(type="text", text="\n".join(result_csv))]
        except Exception as e:
            logger.error(f"Error executing InfluxQL '{query}': {e}")
            return [TextContent(type="text", text=f"Error executing query: {str(e)}")]

    async def run(self):
        """Run the MCP server."""
        logger = self.logger
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            try:
                await self.app.run(
                    read_stream, write_stream, self.app.create_initialization_options()
                )
            except Exception as e:
                logger.error(f"Server error: {str(e)}", exc_info=True)
                raise


async def main(config: Config):
    """Main entry point to run the MCP server."""
    logger = logging.getLogger("openGemini_mcp_server")
    db_server = openGeminiServer(logger, config)

    logger.info("Starting openGemini MCP server...")

    await db_server.run()


if __name__ == "__main__":
    asyncio.run(main(Config.from_env_arguments()))
