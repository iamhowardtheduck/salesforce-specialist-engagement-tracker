#!/usr/bin/env python3
"""
sf_account_opportunities.py ES Connection Diagnostic

This script replicates the exact ES connection logic from sf_account_opportunities.py
to show you why ES indexing is being skipped.

Usage:
    python3 sf_account_es_debug.py
"""

import sys
import os
import logging

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_environment_variables():
    """Test environment variable setup."""
    
    print("🌍 STEP 1: CHECKING ENVIRONMENT VARIABLES")
    print("=" * 50)
    
    env_vars = {
        'ES_CLUSTER_URL': os.environ.get('ES_CLUSTER_URL'),
        'ES_USERNAME': os.environ.get('ES_USERNAME'), 
        'ES_PASSWORD': os.environ.get('ES_PASSWORD'),
        'ES_API_KEY': os.environ.get('ES_API_KEY'),
        'ES_INDEX': os.environ.get('ES_INDEX'),
    }
    
    print("Environment variables:")
    for key, value in env_vars.items():
        if value:
            if key in ['ES_PASSWORD', 'ES_API_KEY']:
                print(f"✅ {key}: [SET - {len(value)} characters]")
            else:
                print(f"✅ {key}: {value}")
        else:
            print(f"❌ {key}: Not set")
    
    # Check if any auth method is available
    has_basic_auth = env_vars['ES_USERNAME'] and env_vars['ES_PASSWORD']
    has_api_key = env_vars['ES_API_KEY']
    
    print(f"\nAuthentication check:")
    print(f"Basic auth (username/password): {'✅' if has_basic_auth else '❌'}")
    print(f"API key auth: {'✅' if has_api_key else '❌'}")
    
    return env_vars, has_basic_auth or has_api_key

def test_config_module():
    """Test the config module ES setup."""
    
    print(f"\n🔧 STEP 2: TESTING CONFIG MODULE")
    print("=" * 40)
    
    try:
        from config import get_elasticsearch_config_from_env, validate_es_config
        print("✅ Config module imported successfully")
        
        # This is exactly what sf_account_opportunities.py does
        print("📋 Getting ES config from environment...")
        es_config = get_elasticsearch_config_from_env()
        
        if es_config:
            print("✅ ES config retrieved from environment")
            print(f"   Cluster URL: {es_config.get('cluster_url', 'None')}")
            print(f"   Index: {es_config.get('index', 'None')}")
            print(f"   Auth type: {es_config.get('auth_type', 'None')}")
        else:
            print("❌ ES config is None")
            return None
        
        # Validate config
        print("🔍 Validating ES config...")
        is_valid, error_msg = validate_es_config(es_config)
        
        if is_valid:
            print("✅ ES config validation passed")
            return es_config
        else:
            print(f"❌ ES config validation failed: {error_msg}")
            return None
            
    except Exception as e:
        print(f"❌ Config module error: {str(e)}")
        import traceback
        print(f"   Full traceback: {traceback.format_exc()}")
        return None

def test_processor_creation(es_config):
    """Test creating the AccountOpportunitiesProcessor."""
    
    print(f"\n👷 STEP 3: TESTING PROCESSOR CREATION")
    print("=" * 45)
    
    try:
        # Import the exact class used
        sys.path.insert(0, '/home/claude')
        from sf_account_opportunities import AccountOpportunitiesProcessor
        
        print("✅ AccountOpportunitiesProcessor imported")
        
        # Create processor with ES config (this is exactly what the script does)
        print("🏗️  Creating processor with ES config...")
        processor = AccountOpportunitiesProcessor(es_config)
        
        print(f"✅ Processor created")
        print(f"   ES config present: {'✅' if processor.es_config else '❌'}")
        print(f"   ES connection object: {'✅' if hasattr(processor, 'es') else '❌'}")
        
        # Check initial ES connection state
        if hasattr(processor, 'es'):
            print(f"   ES object value: {type(processor.es)} = {processor.es}")
        
        return processor
        
    except Exception as e:
        print(f"❌ Processor creation failed: {str(e)}")
        import traceback
        print(f"   Full traceback: {traceback.format_exc()}")
        return None

def test_es_connection(processor):
    """Test the actual ES connection process."""
    
    print(f"\n🔗 STEP 4: TESTING ELASTICSEARCH CONNECTION")
    print("=" * 50)
    
    if not processor:
        print("❌ No processor to test")
        return False
    
    try:
        # This is the exact call made by sf_account_opportunities.py
        print("🔍 Calling processor.connect_elasticsearch()...")
        connection_result = processor.connect_elasticsearch()
        
        print(f"📊 Connection result: {connection_result}")
        
        if connection_result:
            print("✅ ES connection succeeded!")
            
            # Check if ES object is properly set
            if hasattr(processor, 'es') and processor.es:
                print("✅ ES object is available on processor")
                
                # Test basic ES operations
                try:
                    info = processor.es.info()
                    print(f"✅ ES cluster info retrieved: {info.get('cluster_name', 'Unknown')}")
                    
                    health = processor.es.cluster.health()
                    print(f"✅ ES cluster health: {health.get('status', 'Unknown')}")
                    
                    return True
                    
                except Exception as e:
                    print(f"❌ ES operations failed: {str(e)}")
                    return False
                    
            else:
                print("❌ ES object not properly set on processor")
                return False
        else:
            print("❌ ES connection failed!")
            
            # Try to get more details about why
            print("\n🔍 Investigating connection failure...")
            
            if not processor.es_config:
                print("   Cause: No ES config on processor")
            else:
                print("   ES config exists, connection failed for other reason")
                
                # Try manual connection with same config
                try:
                    from elasticsearch import Elasticsearch
                    
                    connection_params = {
                        'verify_certs': False,
                        'request_timeout': 30
                    }
                    
                    if processor.es_config.get('auth_type') == 'api_key':
                        connection_params['api_key'] = processor.es_config['api_key']
                    else:
                        connection_params['basic_auth'] = (processor.es_config['username'], processor.es_config['password'])
                    
                    print(f"   Trying manual connection to: {processor.es_config['cluster_url']}")
                    es = Elasticsearch([processor.es_config['cluster_url']], **connection_params)
                    
                    # Test connection
                    info = es.info()
                    print(f"   ✅ Manual connection works: {info.get('cluster_name', 'Unknown')}")
                    print(f"   ❌ Issue is in processor.connect_elasticsearch() method")
                    
                except Exception as e:
                    print(f"   ❌ Manual connection also failed: {str(e)}")
            
            return False
    
    except Exception as e:
        print(f"❌ Error testing ES connection: {str(e)}")
        import traceback
        print(f"   Full traceback: {traceback.format_exc()}")
        return False

def test_indexing_conditions(processor, json_only_flag=False):
    """Test the conditions that determine if indexing happens."""
    
    print(f"\n🎯 STEP 5: TESTING INDEXING CONDITIONS")
    print("=" * 45)
    
    print(f"Simulating sf_account_opportunities.py indexing logic...")
    
    # These are the exact conditions from the script
    condition1 = not json_only_flag
    condition2 = processor and hasattr(processor, 'es') and processor.es
    
    print(f"Condition 1 - not args.json_only: {condition1}")
    print(f"Condition 2 - processor.es exists: {condition2}")
    
    overall_condition = condition1 and condition2
    print(f"Overall condition (AND): {overall_condition}")
    
    if overall_condition:
        print("✅ INDEXING WOULD HAPPEN - Both conditions met")
        print("   You should see: '🔍 Indexing to Elasticsearch...'")
    else:
        print("❌ INDEXING WOULD BE SKIPPED")
        
        if not condition1:
            print("   Reason: Using --json-only flag")
        if not condition2:
            print("   Reason: No Elasticsearch connection (processor.es is None)")
            print("   This means the tool silently fell back to JSON-only mode")
    
    return overall_condition

def simulate_script_flow():
    """Simulate the exact flow of sf_account_opportunities.py."""
    
    print(f"\n🎬 STEP 6: SIMULATING SCRIPT FLOW")
    print("=" * 40)
    
    print("This simulates what sf_account_opportunities.py does:")
    print()
    
    # Simulate args
    print("1. Parse arguments (assuming no --json-only)")
    json_only = False
    
    # Step 2: Get ES config
    print("2. Get ES config from environment")
    es_config = test_config_module()
    
    if not es_config:
        print("   ❌ WOULD SKIP ES: No config available")
        return
    
    # Step 3: Create processor
    print("3. Create processor")
    processor = test_processor_creation(es_config)
    
    if not processor:
        print("   ❌ WOULD FAIL: Cannot create processor")
        return
    
    # Step 4: Connect to Salesforce (skipping this)
    print("4. Connect to Salesforce (assumed working)")
    
    # Step 5: ES connection logic
    print("5. ES connection logic")
    print("   if not args.json_only and es_config:")
    
    if not json_only and es_config:
        print("   ✅ Condition met, trying ES connection...")
        print("   🔍 Connecting to Elasticsearch...")
        
        if not test_es_connection(processor):
            print("   ⚠️  Failed to connect to Elasticsearch, switching to JSON-only mode")
            json_only = True
        else:
            print("   ✅ ES connection successful")
    else:
        print("   ❌ Condition not met, skipping ES connection")
    
    # Step 6: Query and process data (skipping)
    print("6. Query and process data (assumed working)")
    
    # Step 7: Indexing decision
    print("7. Indexing decision")
    print("   if not args.json_only and processor.es:")
    
    indexing_would_happen = test_indexing_conditions(processor, json_only)
    
    if indexing_would_happen:
        print("   🔍 Indexing to Elasticsearch...")
        print("   ✅ Successfully indexed X opportunities to Elasticsearch")
    else:
        print("   (No indexing message - goes straight to JSON)")

def main():
    """Main diagnostic function."""
    
    print("🔍 sf_account_opportunities.py ES CONNECTION DEBUG")
    print("=" * 55)
    print("This diagnoses why ES indexing is being skipped")
    print()
    
    # Test each step
    env_vars, auth_available = test_environment_variables()
    
    if not auth_available:
        print(f"\n❌ PROBLEM FOUND: No authentication configured")
        print("💡 Set either:")
        print("   export ES_USERNAME='user' ES_PASSWORD='pass'")
        print("   OR")
        print("   export ES_API_KEY='your_api_key'")
        return
    
    es_config = test_config_module()
    if not es_config:
        print(f"\n❌ PROBLEM FOUND: Config module failed")
        return
    
    processor = test_processor_creation(es_config)
    if not processor:
        print(f"\n❌ PROBLEM FOUND: Cannot create processor")
        return
    
    es_connected = test_es_connection(processor)
    
    indexing_possible = test_indexing_conditions(processor)
    
    # Simulate full flow
    simulate_script_flow()
    
    print(f"\n🎯 DIAGNOSIS SUMMARY")
    print("=" * 25)
    
    if es_connected and indexing_possible:
        print("✅ ES indexing should work!")
        print("💡 If it's still not working, run sf_account_opportunities.py with --verbose")
        print("   and look for the '🔍 Indexing to Elasticsearch...' message")
    else:
        print("❌ Found the problem!")
        if not es_connected:
            print("   Issue: Elasticsearch connection failed")
            print("   The tool silently falls back to JSON-only mode")
        else:
            print("   Issue: Indexing conditions not met")
    
    print(f"\n🔧 NEXT STEPS:")
    print("1. Fix any issues found above")
    print("2. Run: python3 sf_account_opportunities.py 'account_url' --verbose")
    print("3. Look for '🔍 Connecting to Elasticsearch...' message")
    print("4. Look for '🔍 Indexing to Elasticsearch...' message")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n👋 Diagnostic cancelled")
    except Exception as e:
        print(f"\n💥 Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
