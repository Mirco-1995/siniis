#!/usr/bin/env python3
"""
Async MongoDB checker for opiRunner v1.2.0

Provides non-blocking async operations for MongoDB polling.
Requires: motor (async MongoDB driver)

Install with: pip install motor
"""

import asyncio
import time
from typing import Dict, Tuple

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False
    AsyncIOMotorClient = None


# Global async client pool
_async_mongo_clients: Dict[str, AsyncIOMotorClient] = {}


def get_async_mongo_client(mongo_config: Dict) -> AsyncIOMotorClient:
    """Get or create async MongoDB client from connection pool.

    Args:
        mongo_config: MongoDB configuration dict

    Returns:
        AsyncIOMotorClient instance
    """
    if not MOTOR_AVAILABLE:
        raise ImportError("motor module not available. Install with: pip install motor")

    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_user = mongo_config.get('user', '')
    mongo_pass = mongo_config.get('password', '')

    conn_key = f"{mongo_host}:{mongo_port}:{mongo_user}"

    # Return existing client if available
    if conn_key in _async_mongo_clients:
        return _async_mongo_clients[conn_key]

    # Create new async client
    if mongo_user and mongo_pass:
        client = AsyncIOMotorClient(
            mongo_host, mongo_port,
            username=mongo_user, password=mongo_pass,
            maxPoolSize=50, minPoolSize=10,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000
        )
    else:
        client = AsyncIOMotorClient(
            mongo_host, mongo_port,
            maxPoolSize=50, minPoolSize=10,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000
        )

    _async_mongo_clients[conn_key] = client
    return client


async def close_all_async_mongo_clients():
    """Close all async MongoDB clients in the pool."""
    for client in _async_mongo_clients.values():
        client.close()
    _async_mongo_clients.clear()


async def verify_mongodb_count_with_polling_async(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Async post-check: Verify MongoDB document count with polling and stability check.

    Non-blocking version using asyncio. Allows other operations to continue while waiting.

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict
        poll_interval_sec: Seconds between polls (default: 30)
        stability_timeout_min: Minutes of stable count before giving up (default: 5)
        max_wait_min: Maximum total wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, message: str)
    """
    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')

    try:
        # Get async client from pool
        client = get_async_mongo_client(mongo_config)

        # Get collection
        db = client[mongo_db]
        collection = db['anagrafiche']

        # Query filter
        query = {
            "rataEmissione": rata,
            "tipoSpesa": "SPT"
        }

        # Polling variables
        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        stability_timeout_sec = stability_timeout_min * 60

        last_count = None
        last_count_time = None
        poll_count = 0

        messages = []
        messages.append(f"Expected document count: {expected_count}")
        messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
        messages.append(f"Collection: anagrafiche, Query: {query}")
        messages.append(f"Poll interval: {poll_interval_sec}s, Stability timeout: {stability_timeout_min}min, Max wait: {max_wait_min}min")
        messages.append(f"Mode: ASYNC (non-blocking)")
        messages.append("-" * 60)

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            # Check max wait time
            if elapsed_sec > max_wait_sec:
                msg = "\n".join(messages)
                msg += f"\n\nERROR: Maximum wait time ({max_wait_min} minutes) exceeded"
                msg += f"\nFinal count: {last_count}, Expected: {expected_count}"
                return False, msg

            # Query MongoDB (async)
            try:
                actual_count = await collection.count_documents(query)
                messages.append(f"Poll #{poll_count} ({elapsed_sec:.0f}s): {actual_count} documents")

                # Check if we reached expected count
                if actual_count == expected_count:
                    msg = "\n".join(messages)
                    msg += f"\n\nSUCCESS: Document count matches expected count ({expected_count})"
                    msg += f"\nTotal time: {elapsed_sec:.1f}s ({poll_count} polls)"
                    return True, msg

                # Check for stability (count hasn't changed)
                if last_count is not None and actual_count == last_count:
                    time_stable = time.time() - last_count_time
                    if time_stable >= stability_timeout_sec:
                        msg = "\n".join(messages)
                        msg += f"\n\nERROR: Document count stable at {actual_count} for {stability_timeout_min} minutes"
                        msg += f"\nExpected: {expected_count}, Actual: {actual_count}, Difference: {actual_count - expected_count}"
                        return False, msg
                else:
                    # Count changed, reset stability timer
                    last_count = actual_count
                    last_count_time = time.time()

            except Exception as e:
                messages.append(f"Poll #{poll_count}: Query error: {e}")

            # Async sleep (non-blocking)
            await asyncio.sleep(poll_interval_sec)

    except Exception as e:
        return False, f"MongoDB connection error: {e}"


def verify_mongodb_count_with_polling_async_wrapper(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Synchronous wrapper for async MongoDB polling.

    Can be called from synchronous code. Creates event loop if needed.

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict
        poll_interval_sec: Seconds between polls
        stability_timeout_min: Minutes of stable count before giving up
        max_wait_min: Maximum total wait time in minutes

    Returns:
        Tuple of (success: bool, message: str)
    """
    if not MOTOR_AVAILABLE:
        return False, "motor module not available. Install with: pip install motor"

    try:
        # Try to get existing event loop
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        # No event loop in current thread, create one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        # Run async function in event loop
        return loop.run_until_complete(
            verify_mongodb_count_with_polling_async(
                rata=rata,
                expected_count=expected_count,
                mongo_config=mongo_config,
                poll_interval_sec=poll_interval_sec,
                stability_timeout_min=stability_timeout_min,
                max_wait_min=max_wait_min
            )
        )
    except Exception as e:
        return False, f"Async polling error: {e}"


# Convenience exports
__all__ = [
    'MOTOR_AVAILABLE',
    'get_async_mongo_client',
    'close_all_async_mongo_clients',
    'verify_mongodb_count_with_polling_async',
    'verify_mongodb_count_with_polling_async_wrapper',
]
