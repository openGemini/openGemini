import argparse
from dataclasses import dataclass
import os


@dataclass
class Config:
    """
    Configuration for the openGemini mcp server.
    """

    host: str
    """
    openGemini host
    """

    port: int
    """
    openGemini port
    """

    user: str
    """
    openGemini username
    """

    password: str
    """
    openGemini password
    """

    database: str
    """
    openGemini database
    """

    @staticmethod
    def from_env_arguments() -> "Config":
        """
        Parse command line arguments.
        """
        parser = argparse.ArgumentParser(description="openGemini MCP Server")

        parser.add_argument(
            "--host",
            type=str,
            help="openGemini host",
            default=os.getenv("OPENGEMINI_HOST", "localhost"),
        )

        parser.add_argument(
            "--port",
            type=int,
            help="openGemini port",
            default=os.getenv("OPENGEMINI_PORT", 8086),
        )

        parser.add_argument(
            "--user",
            type=str,
            help="openGemini username",
            default=os.getenv("OPENGEMINI_USER", ""),
        )

        parser.add_argument(
            "--password",
            type=str,
            help="openGemini password",
            default=os.getenv("OPENGEMINI_PASSWORD", ""),
        )

        parser.add_argument(
            "--database",
            type=str,
            help="openGemini database",
            default=os.getenv("OPENGEMINI_DATABASE", ""),
        )

        args = parser.parse_args()
        return Config(
            host=args.host, port=args.port, user=args.user, password=args.password, database=args.database,
        )
