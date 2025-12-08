#!/usr/bin/env python3
"""
Salesforce Opportunity Explorer - JSON Output

This script queries Salesforce opportunities and outputs JSON to see available fields.
Useful for exploring what fields exist before setting up Elasticsearch indexing.

Usage:
    python sf_explore_json.py <opportunity_url>
    
Example:
    python sf_explore_json.py "https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view"
"""

import sys
import re
import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sf_auth import get_salesforce_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_opportunity_id(url: str) -> Optional[str]:
    """
    Extract Salesforce Opportunity ID from URL.
    
    Args:
        url: Salesforce opportunity URL
        
    Returns:
        Opportunity ID or None if not found
    """
    # Pattern for Salesforce opportunity ID (15 or 18 characters starting with 006)
    patterns = [
        r'/([A-Za-z0-9]{15,18})',  # Generic ID pattern
        r'/Opportunity/([A-Za-z0-9]{15,18})',  # Explicit opportunity pattern
        r'006[A-Za-z0-9]{12,15}',  # Opportunity-specific pattern
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            opportunity_id = match.group(1) if len(match.groups()) > 0 else match.group(0)
            # Ensure it starts with 006 (Opportunity prefix)
            if opportunity_id.startswith('006') and len(opportunity_id) >= 15:
                return opportunity_id
    
    logger.error(f"Could not extract opportunity ID from URL: {url}")
    return None

def describe_opportunity_object(sf) -> Dict[str, Any]:
    """
    Get field descriptions for the Opportunity object.
    
    Args:
        sf: Authenticated Salesforce connection
        
    Returns:
        Dictionary with field information
    """
    try:
        describe_result = sf.Opportunity.describe()
        
        # Extract field information
        fields = {}
        for field in describe_result['fields']:
            fields[field['name']] = {
                'label': field['label'],
                'type': field['type'],
                'custom': field.get('custom', False),
                'updateable': field.get('updateable', False),
                'queryable': field.get('queryable', False)
            }
        
        return {
            'object_info': {
                'name': describe_result['name'],
                'label': describe_result['label'],
                'custom': describe_result.get('custom', False)
            },
            'fields': fields
        }
        
    except Exception as e:
        logger.error(f"Error describing Opportunity object: {str(e)}")
        return {}

def query_opportunity_basic(sf, opportunity_id: str) -> Optional[Dict[str, Any]]:
    """
    Query Salesforce for basic opportunity data (fields that should always exist).
    
    Args:
        sf: Authenticated Salesforce connection
        opportunity_id: Salesforce opportunity ID
        
    Returns:
        Dictionary with opportunity data or None if not found
    """
    try:
        # Basic SOQL query with standard fields only
        soql_query = f"""
        SELECT 
            Id,
            Name,
            Account.Name,
            CloseDate,
            Amount,
            StageName,
            Type,
            CreatedDate,
            LastModifiedDate,
            Owner.Name
        FROM Opportunity 
        WHERE Id = '{opportunity_id}'
        """
        
        logger.info(f"Querying Salesforce for opportunity: {opportunity_id}")
        result = sf.query(soql_query)
        
        if result['totalSize'] == 0:
            logger.error(f"No opportunity found with ID: {opportunity_id}")
            return None
            
        opportunity = result['records'][0]
        logger.info(f"Successfully retrieved opportunity: {opportunity['Name']}")
        return opportunity
        
    except Exception as e:
        logger.error(f"Error querying Salesforce: {str(e)}")
        return None

def query_opportunity_all_fields(sf, opportunity_id: str) -> Optional[Dict[str, Any]]:
    """
    Query Salesforce for opportunity with all queryable fields.
    
    Args:
        sf: Authenticated Salesforce connection
        opportunity_id: Salesforce opportunity ID
        
    Returns:
        Dictionary with opportunity data or None if not found
    """
    try:
        # Get field descriptions to build comprehensive query
        describe_result = sf.Opportunity.describe()
        
        # Get all queryable fields
        queryable_fields = []
        for field in describe_result['fields']:
            if field.get('queryable', False) and field['type'] not in ['base64']:
                # Skip relationship fields that might cause issues
                if '.' not in field['name']:
                    queryable_fields.append(field['name'])
        
        # Add some common relationship fields manually
        relationship_fields = [
            'Account.Name',
            'Account.Id', 
            'Owner.Name',
            'Owner.Id'
        ]
        
        all_fields = queryable_fields + relationship_fields
        fields_str = ',\n            '.join(all_fields)
        
        soql_query = f"""
        SELECT 
            {fields_str}
        FROM Opportunity 
        WHERE Id = '{opportunity_id}'
        """
        
        logger.info(f"Querying with {len(all_fields)} fields...")
        result = sf.query(soql_query)
        
        if result['totalSize'] == 0:
            logger.error(f"No opportunity found with ID: {opportunity_id}")
            return None
            
        opportunity = result['records'][0]
        logger.info(f"Successfully retrieved opportunity with all fields")
        return opportunity
        
    except Exception as e:
        logger.error(f"Error in comprehensive query: {str(e)}")
        # Fall back to basic query
        logger.info("Falling back to basic field query...")
        return query_opportunity_basic(sf, opportunity_id)

def main():
    """Main execution function."""
    if len(sys.argv) != 2:
        print("Usage: python sf_explore_json.py <opportunity_url>")
        print("Example: python sf_explore_json.py 'https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view'")
        sys.exit(1)
    
    opportunity_url = sys.argv[1]
    logger.info(f"Exploring opportunity URL: {opportunity_url}")
    
    # Step 1: Extract opportunity ID from URL
    opportunity_id = extract_opportunity_id(opportunity_url)
    if not opportunity_id:
        logger.error("Failed to extract opportunity ID from URL")
        sys.exit(1)
    
    print(f"🔍 Extracted Opportunity ID: {opportunity_id}")
    
    # Step 2: Connect to Salesforce
    try:
        sf = get_salesforce_connection()
        logger.info("Successfully connected to Salesforce")
        print("✅ Connected to Salesforce")
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {str(e)}")
        sys.exit(1)
    
    # Step 3: Describe the Opportunity object
    print("\n📋 Describing Opportunity object...")
    describe_info = describe_opportunity_object(sf)
    
    if describe_info:
        print(f"   Object: {describe_info['object_info']['label']}")
        print(f"   Total fields: {len(describe_info['fields'])}")
        
        # Show custom fields
        custom_fields = [name for name, info in describe_info['fields'].items() if info.get('custom', False)]
        if custom_fields:
            print(f"   Custom fields found: {len(custom_fields)}")
            print("   Custom field examples:", ', '.join(custom_fields[:5]))
        else:
            print("   No custom fields found")
    
    # Step 4: Query opportunity data
    print(f"\n📊 Querying opportunity data...")
    
    # Try comprehensive query first, fall back to basic if needed
    opportunity_data = query_opportunity_all_fields(sf, opportunity_id)
    
    if not opportunity_data:
        logger.error("Failed to retrieve opportunity data from Salesforce")
        sys.exit(1)
    
    # Step 5: Output results
    output = {
        'extraction_info': {
            'opportunity_id': opportunity_id,
            'url': opportunity_url,
            'extracted_at': datetime.utcnow().isoformat(),
            'source': 'salesforce'
        },
        'field_description': describe_info,
        'opportunity_data': opportunity_data
    }
    
    # Pretty print JSON
    print(f"\n🎯 Complete Opportunity Data:")
    print("=" * 60)
    print(json.dumps(output, indent=2, default=str))
    
    # Also save to file
    filename = f"opportunity_{opportunity_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\n💾 Data saved to: {filename}")
    
    # Summary of key fields
    print(f"\n📈 Key Opportunity Fields:")
    key_fields = ['Id', 'Name', 'Account', 'Amount', 'CloseDate', 'StageName']
    for field in key_fields:
        if field in opportunity_data:
            value = opportunity_data[field]
            if isinstance(value, dict) and 'Name' in value:
                value = value['Name']  # For Account.Name
            print(f"   {field}: {value}")
    
    # Show available custom fields in this record
    custom_data = {k: v for k, v in opportunity_data.items() if k.endswith('__c') and v is not None}
    if custom_data:
        print(f"\n🔧 Custom Fields in this Opportunity:")
        for field, value in custom_data.items():
            print(f"   {field}: {value}")
    else:
        print(f"\n🔧 No custom fields with data found in this opportunity")

if __name__ == "__main__":
    main()
