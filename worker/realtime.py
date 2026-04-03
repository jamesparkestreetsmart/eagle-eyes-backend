# worker/realtime.py
import asyncio
from dotenv import load_dotenv
load_dotenv()

from services.realtime_listener import run_listener

if __name__ == "__main__":
    asyncio.run(run_listener())