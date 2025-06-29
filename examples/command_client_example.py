"""
Example client for sending commands to the Ravenfall MultiChat server via HTTP.

This script demonstrates how to send commands to the chat bot using the HTTP endpoint.
Make sure the server is running and the COMMAND_SERVER_HOST and COMMAND_SERVER_PORT
environment variables are properly set.
"""
import asyncio
import aiohttp
import json
import os
from typing import Dict, Any, Optional

# Configuration - Update these values to match your setup
COMMAND_SERVER_HOST = os.getenv("COMMAND_SERVER_HOST", "localhost")
COMMAND_SERVER_PORT = int(os.getenv("COMMAND_SERVER_PORT", "8080"))
BASE_URL = f"http://{COMMAND_SERVER_HOST}:{COMMAND_SERVER_PORT}"

async def send_command(
    text: str,
    user_id: str = "example_user_id",
    user_name: str = "example_user",
    channel_id: str = "example_channel_id",
    channel_name: str = "example_channel"
) -> Dict[str, Any]:
    """
    Send a command to the Ravenfall MultiChat server.
    
    Args:
        text: The command text to send (e.g., "?ping", "?sailall")
        user_id: The ID of the user sending the command
        user_name: The username of the user sending the command
        channel_id: The ID of the channel where the command should be processed
        channel_name: The name of the channel
        
    Returns:
        dict: The JSON response from the server
    """
    url = f"{BASE_URL}/command"
    payload = {
        "text": text,
        "user_id": user_id,
        "user_name": user_name,
        "channel_id": channel_id,
        "channel_name": channel_name
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                response_data = await response.json()
                return {
                    "status": response.status,
                    "data": response_data
                }
    except Exception as e:
        return {
            "status": 500,
            "error": f"Failed to send command: {str(e)}"
        }

async def main():
    """Example usage of the command client."""
    # Example 1: Send a ping command
    print("Sending ping command...")
    response = await send_command(
        text="?ping",
        user_id="12345",
        user_name="testuser",
        channel_id="67890",
        channel_name="testchannel"
    )
    print(f"Ping response: {json.dumps(response, indent=2)}")
    
    # Example 2: Send a sailall command
    print("\nSending sailall command...")
    response = await send_command(
        text="?sailall",
        user_id="12345",
        user_name="testuser",
        channel_id="67890",
        channel_name="testchannel"
    )
    print(f"Sailall response: {json.dumps(response, indent=2)}")
    
    # Example 3: Send a custom message
    print("\nSending custom message...")
    response = await send_command(
        text="?say Hello from the HTTP client!",
        user_id="12345",
        user_name="testuser",
        channel_id="67890",
        channel_name="testchannel"
    )
    print(f"Custom message response: {json.dumps(response, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
