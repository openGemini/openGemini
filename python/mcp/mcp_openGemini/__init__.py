from mcp_openGemini.config import Config
from . import server
import asyncio


def main():
    """Main entry point for the package."""
    config = Config.from_env_arguments()
    asyncio.run(server.main(config))


# Expose important items at package level
__all__ = ["main", "server"]
