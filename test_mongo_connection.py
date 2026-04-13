#!/usr/bin/env python3
"""
Test MongoDB Connection using MONGODB_URI environment variable

This script tests MongoDB connectivity and authentication using the MONGODB_URI
environment variable, exactly as opiRunner does.

Usage:
    python test_mongo_connection.py

Requirements:
    - pymongo installed: pip install pymongo
    - MONGODB_URI environment variable set
"""

import os
import sys
from datetime import datetime

def test_mongodb_connection():
    """Test MongoDB connection using MONGODB_URI from environment"""

    print("=" * 80)
    print("MongoDB Connection Test")
    print("=" * 80)
    print()

    # Get MONGODB_URI from environment
    mongo_uri = os.environ.get('MONGODB_URI', '')

    if not mongo_uri:
        print("❌ ERROR: MONGODB_URI environment variable not set")
        print()
        print("Please set it first:")
        print("  Linux/Mac:  export MONGODB_URI='mongodb://...'")
        print("  Windows:    set MONGODB_URI=mongodb://...")
        print()
        return False

    # Mask credentials in URI for display
    display_uri = mongo_uri
    if '@' in display_uri:
        # Hide password: mongodb://user:password@host -> mongodb://user:***@host
        parts = display_uri.split('@')
        if '://' in parts[0]:
            protocol_user = parts[0].split('://')
            if ':' in protocol_user[1]:
                user = protocol_user[1].split(':')[0]
                display_uri = f"{protocol_user[0]}://{user}:***@{parts[1]}"

    print(f"MONGODB_URI: {display_uri}")
    print()

    # Check if pymongo is available
    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
        print("✓ pymongo module found")
    except ImportError as e:
        print(f"❌ ERROR: pymongo module not available")
        print(f"   {e}")
        print()
        print("Install with: pip install pymongo")
        print()
        return False

    print()
    print("-" * 80)
    print("Testing connection...")
    print("-" * 80)
    print()

    try:
        # Create MongoDB client with same parameters as opiRunner
        print("1. Creating MongoClient...")
        client = MongoClient(
            mongo_uri,
            maxPoolSize=50,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=0,
            waitQueueTimeoutMS=900000,
            maxIdleTimeMS=3600000
        )
        print("   ✓ MongoClient created")
        print()

        # Test connection
        print("2. Testing server connection...")
        client.admin.command('ping')
        print("   ✓ Server ping successful")
        print()

        # Get server info
        print("3. Retrieving server information...")
        server_info = client.server_info()
        print(f"   ✓ MongoDB version: {server_info.get('version', 'unknown')}")
        print()

        # List databases
        print("4. Listing databases...")
        db_list = client.list_database_names()
        print(f"   ✓ Found {len(db_list)} databases:")
        for db_name in db_list:
            print(f"     - {db_name}")
        print()

        # Extract database name from URI
        db_name = None
        if '/' in mongo_uri.split('@')[-1]:
            db_part = mongo_uri.split('@')[-1].split('/')[1]
            db_name = db_part.split('?')[0] if '?' in db_part else db_part

        if db_name:
            print(f"5. Testing access to database '{db_name}'...")
            try:
                db = client[db_name]
                collection_names = db.list_collection_names()
                print(f"   ✓ Database '{db_name}' accessible")
                print(f"   ✓ Found {len(collection_names)} collections")
                if collection_names:
                    print("   Collections:")
                    for coll_name in sorted(collection_names)[:10]:  # Show max 10
                        print(f"     - {coll_name}")
                    if len(collection_names) > 10:
                        print(f"     ... and {len(collection_names) - 10} more")
                print()
            except Exception as e:
                print(f"   ⚠ Warning: Could not list collections: {e}")
                print()

        # Test authentication by running a command
        print("6. Verifying authentication...")
        try:
            # This will fail if not authenticated
            client.admin.command('listDatabases')
            print("   ✓ Authentication successful")
            print()
        except OperationFailure as e:
            if e.code == 13:  # Unauthorized
                print(f"   ❌ Authentication failed: Unauthorized")
                print(f"   Error: {e}")
                print()
                return False
            elif e.code == 18:  # AuthenticationFailed
                print(f"   ❌ Authentication failed: Invalid credentials")
                print(f"   Error: {e}")
                print()
                return False
            else:
                raise

        # Close connection
        print("7. Closing connection...")
        client.close()
        print("   ✓ Connection closed")
        print()

        print("=" * 80)
        print("✓ ALL TESTS PASSED")
        print("=" * 80)
        print()
        print("MongoDB connection is working correctly!")
        print("opiRunner should be able to connect using the same MONGODB_URI.")
        print()

        return True

    except ServerSelectionTimeoutError as e:
        print(f"❌ ERROR: Server selection timeout")
        print(f"   {e}")
        print()
        print("Possible causes:")
        print("  - MongoDB server is not running")
        print("  - Incorrect host/port in MONGODB_URI")
        print("  - Firewall blocking connection")
        print("  - Network connectivity issues")
        print()
        return False

    except ConnectionFailure as e:
        print(f"❌ ERROR: Connection failed")
        print(f"   {e}")
        print()
        print("Possible causes:")
        print("  - MongoDB server is not running")
        print("  - Incorrect connection parameters")
        print()
        return False

    except OperationFailure as e:
        print(f"❌ ERROR: Operation failed")
        print(f"   Error code: {e.code}")
        print(f"   Error: {e}")
        print()

        if e.code == 18:  # AuthenticationFailed
            print("Authentication failed!")
            print()
            print("Possible causes:")
            print("  - Incorrect username in MONGODB_URI")
            print("  - Incorrect password in MONGODB_URI")
            print("  - User does not exist in the specified database")
            print("  - User lacks required permissions")
            print()
            print("Check your MONGODB_URI format:")
            print("  mongodb://username:password@host:port/database?options")
            print()
        elif e.code == 13:  # Unauthorized
            print("Authorization failed!")
            print()
            print("The user authenticated but lacks permissions.")
            print("Check user roles and permissions in MongoDB.")
            print()

        return False

    except Exception as e:
        print(f"❌ ERROR: Unexpected error")
        print(f"   {type(e).__name__}: {e}")
        print()
        import traceback
        traceback.print_exc()
        print()
        return False


def check_anagrafiche_collection():
    """Check if anagrafiche collection exists and contains data"""

    print("=" * 80)
    print("Anagrafiche Collection Check")
    print("=" * 80)
    print()

    # Get MONGODB_URI from environment
    mongo_uri = os.environ.get('MONGODB_URI', '')

    if not mongo_uri:
        print("❌ ERROR: MONGODB_URI environment variable not set")
        return False

    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
    except ImportError:
        print("❌ ERROR: pymongo module not available")
        return False

    try:
        # Create MongoDB client
        print("1. Connecting to MongoDB...")
        client = MongoClient(
            mongo_uri,
            maxPoolSize=50,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=0,
            waitQueueTimeoutMS=900000,
            maxIdleTimeMS=3600000
        )
        client.admin.command('ping')
        print("   ✓ Connected")
        print()

        # Extract database name from URI
        db_name = None
        if '/' in mongo_uri.split('@')[-1]:
            db_part = mongo_uri.split('@')[-1].split('/')[1]
            db_name = db_part.split('?')[0] if '?' in db_part else db_part

        if not db_name:
            print("❌ ERROR: Could not extract database name from MONGODB_URI")
            client.close()
            return False

        db = client[db_name]

        # Check if anagrafiche collection exists
        print(f"2. Checking 'anagrafiche' collection in database '{db_name}'...")
        collection_names = db.list_collection_names()

        if 'anagrafiche' not in collection_names:
            print("   ❌ Collection 'anagrafiche' NOT FOUND")
            print(f"   Available collections: {', '.join(collection_names)}")
            print()
            client.close()
            return False

        print("   ✓ Collection 'anagrafiche' exists")
        print()

        # Count total documents
        print("3. Counting documents in 'anagrafiche'...")
        anagrafiche = db['anagrafiche']
        total_count = anagrafiche.count_documents({})
        print(f"   ✓ Total documents: {total_count}")
        print()

        if total_count == 0:
            print("   ⚠ WARNING: Collection is EMPTY")
            print()
            client.close()
            return True  # Collection exists but is empty

        # Count by tipoSpesa
        print("4. Counting documents by tipoSpesa...")
        tipo_spesa_counts = {}
        for tipo_spesa in ['SPT', 'NON_SPT', 'ALTRO']:
            count = anagrafiche.count_documents({"tipoSpesa": tipo_spesa})
            if count > 0:
                tipo_spesa_counts[tipo_spesa] = count
                print(f"   - tipoSpesa '{tipo_spesa}': {count}")

        if not tipo_spesa_counts:
            print("   (No documents with recognized tipoSpesa values)")
        print()

        # Count by rataEmissione (show top 10)
        print("5. Counting documents by rataEmissione (top 10)...")
        pipeline = [
            {"$group": {"_id": "$rataEmissione", "count": {"$sum": 1}}},
            {"$sort": {"_id": -1}},
            {"$limit": 10}
        ]
        rata_counts = list(anagrafiche.aggregate(pipeline))

        if rata_counts:
            for item in rata_counts:
                rata = item['_id']
                count = item['count']
                print(f"   - Rata {rata}: {count} documents")
        else:
            print("   (No documents with rataEmissione field)")
        print()

        # Sample one document
        print("6. Sample document (first one)...")
        sample = anagrafiche.find_one({})
        if sample:
            print("   Keys present in document:")
            for key in sorted(sample.keys()):
                if key == '_id':
                    continue
                value = sample[key]
                if isinstance(value, str) and len(value) > 50:
                    value_display = value[:50] + "..."
                else:
                    value_display = value
                print(f"     - {key}: {value_display}")
        print()

        # Close connection
        client.close()

        print("=" * 80)
        print("✓ ANAGRAFICHE CHECK COMPLETED")
        print("=" * 80)
        print()
        print(f"Summary:")
        print(f"  - Total documents: {total_count}")
        print(f"  - TipoSpesa breakdown: {tipo_spesa_counts}")
        print(f"  - Rate found: {len(rata_counts)}")
        print()

        return True

    except ServerSelectionTimeoutError as e:
        print(f"❌ ERROR: Server selection timeout: {e}")
        return False
    except ConnectionFailure as e:
        print(f"❌ ERROR: Connection failed: {e}")
        return False
    except OperationFailure as e:
        print(f"❌ ERROR: Operation failed (code {e.code}): {e}")
        return False
    except Exception as e:
        print(f"❌ ERROR: Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""

    start_time = datetime.now()
    print()

    # Run connection test
    success = test_mongodb_connection()

    if success:
        print()
        # Run anagrafiche collection check
        anagrafiche_success = check_anagrafiche_collection()
        success = success and anagrafiche_success

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"All tests completed in {duration:.2f} seconds")
    print()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
