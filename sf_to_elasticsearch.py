#!/usr/bin/env python3
"""
Salesforce to Elasticsearch Integration Script

This script queries Salesforce opportunities and ingests the data into Elasticsearch.
It uses the sf_auth module for authentication and retrieves specific opportunity fields.

Usage:
    python sf_to_elasticsearch.py <opportunity_url>
    
Example:
    python sf_to_elasticsearch.py "https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view"
"""

import sys
import re
import json
import logging
from datetime import datetime
from urllib.parse import urlparse
from typing import Optional, Dict, Any

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, AuthenticationException, RequestError
from sf_auth import get_salesforce_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sf_to_es.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Elasticsearch configuration
ES_CONFIG = None  # Will be set interactively

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

def query_opportunity_data(sf, opportunity_id: str) -> Optional[Dict[str, Any]]:
    """
    Query Salesforce for opportunity data.
    
    Args:
        sf: Authenticated Salesforce connection
        opportunity_id: Salesforce opportunity ID
        
    Returns:
        Dictionary with opportunity data or None if not found
    """
    try:
        # SOQL query to get required fields
        soql_query = f"""
        SELECT 
            Id,
            Name,
            Account.Name,
            CloseDate,
            Amount,
            TCV__c
        FROM Opportunity 
        WHERE Id = '{opportunity_id}'
        """
        
        logger.info(f"Querying Salesforce for opportunity: {opportunity_id}")
        result = sf.query(soql_query)
        
        if result['totalSize'] == 0:
            logger.error(f"No opportunity found with ID: {opportunity_id}")
            return None
            
        opportunity = result['records'][0]
        
        # Extract and format the data
        data = {
            'opportunity_id': opportunity['Id'],
            'opportunity_name': opportunity['Name'],
            'account_name': opportunity['Account']['Name'] if opportunity.get('Account') else None,
            'close_date': opportunity['CloseDate'],
            'amount': opportunity['Amount'],
            'tcv_amount': opportunity.get('TCV__c'),
            'extracted_at': datetime.utcnow().isoformat(),
            'source': 'salesforce'
        }
        
        logger.info(f"Successfully retrieved opportunity data: {data['opportunity_name']}")
        return data
        
    except Exception as e:
        logger.error(f"Error querying Salesforce: {str(e)}")
        return None

def connect_elasticsearch(es_config: Dict[str, Any]) -> Optional[Elasticsearch]:
    """
    Create Elasticsearch connection.
    
    Args:
        es_config: Dictionary with Elasticsearch configuration
    
    Returns:
        Elasticsearch client or None if connection fails
    """
    try:
        # Prepare connection parameters
        connection_params = {
            'verify_certs': es_config.get('verify_certs', False),
            'request_timeout': 30
        }
        
        # Add authentication
        if es_config.get('auth_type') == 'api_key':
            connection_params['api_key'] = es_config['api_key']
        else:
            connection_params['basic_auth'] = (es_config['username'], es_config['password'])
        
        es = Elasticsearch(
            [es_config['cluster_url']],
            **connection_params
        )
        
        # Test connection
        info = es.info()
        logger.info(f"Connected to Elasticsearch cluster: {info['name']}")
        return es
        
    except ConnectionError:
        logger.error(f"Could not connect to Elasticsearch at {es_config['cluster_url']}")
        return None
    except AuthenticationException:
        logger.error("Elasticsearch authentication failed. Please check credentials.")
        return None
    except Exception as e:
        logger.error(f"Unexpected error connecting to Elasticsearch: {str(e)}")
        return None

def index_document(es: Elasticsearch, data: Dict[str, Any], es_config: Dict[str, Any]) -> bool:
    """
    Index document in Elasticsearch.
    
    Args:
        es: Elasticsearch client
        data: Document data to index
        es_config: Elasticsearch configuration
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use opportunity_id as document ID to prevent duplicates
        doc_id = data['opportunity_id']
        
        response = es.index(
            index=es_config['index'],
            id=doc_id,
            body=data
        )
        
        logger.info(f"Document indexed successfully: {response['_id']} (result: {response['result']})")
        return True
        
    except RequestError as e:
        logger.error(f"Elasticsearch indexing error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during indexing: {str(e)}")
        return False

def create_index_if_not_exists(es: Elasticsearch, es_config: Dict[str, Any]) -> bool:
    """
    Create index with appropriate mapping if it doesn't exist.
    
    Args:
        es: Elasticsearch client
        es_config: Elasticsearch configuration
        
    Returns:
        True if index exists or was created successfully
    """
    index_name = es_config['index']
    
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' already exists")
            return True
            
        # Define mapping for the index
        mapping = {
            "mappings": {
                "properties": {
                    "opportunity_id": {"type": "keyword"},
                    "opportunity_name": {"type": "text"},
                    "account_name": {"type": "text"},
                    "close_date": {"type": "date"},
                    "amount": {"type": "double"},
                    "tcv_amount": {"type": "double"},
                    "extracted_at": {"type": "date"},
                    "source": {"type": "keyword"}
                }
            }
        }
        
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"Created index '{index_name}' with mapping")
        return True
        
    except Exception as e:
        logger.error(f"Error creating index: {str(e)}")
        return False

def main():
    """Main execution function."""
    if len(sys.argv) != 2:
        print("Usage: python sf_to_elasticsearch.py <opportunity_url>")
        print("Example: python sf_to_elasticsearch.py 'https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view'")
        sys.exit(1)
    
    opportunity_url = sys.argv[1]
    logger.info(f"Processing opportunity URL: {opportunity_url}")
    
    # Step 0: Get Elasticsearch configuration
    from config import get_elasticsearch_config, validate_es_config
    
    try:
        es_config = get_elasticsearch_config()
        is_valid, error_msg = validate_es_config(es_config)
        if not is_valid:
            logger.error(f"Invalid configuration: {error_msg}")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nConfiguration cancelled by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error getting configuration: {str(e)}")
        sys.exit(1)
    
    # Step 1: Extract opportunity ID from URL
    opportunity_id = extract_opportunity_id(opportunity_url)
    if not opportunity_id:
        logger.error("Failed to extract opportunity ID from URL")
        sys.exit(1)
    
    # Step 2: Connect to Salesforce
    try:
        sf = get_salesforce_connection()
        logger.info("Successfully connected to Salesforce")
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {str(e)}")
        sys.exit(1)
    
    # Step 3: Query opportunity data
    opportunity_data = query_opportunity_data(sf, opportunity_id)
    if not opportunity_data:
        logger.error("Failed to retrieve opportunity data from Salesforce")
        sys.exit(1)
    
    # Step 4: Connect to Elasticsearch
    es = connect_elasticsearch(es_config)
    if not es:
        logger.error("Failed to connect to Elasticsearch")
        sys.exit(1)
    
    # Step 5: Create index if it doesn't exist
    if not create_index_if_not_exists(es, es_config):
        logger.error("Failed to create or verify Elasticsearch index")
        sys.exit(1)
    
    # Step 6: Index the document
    if index_document(es, opportunity_data, es_config):
        logger.info("Successfully processed opportunity and indexed to Elasticsearch")
        print(f"\nSuccess! Opportunity '{opportunity_data['opportunity_name']}' has been indexed to Elasticsearch.")
        print(f"Document ID: {opportunity_data['opportunity_id']}")
        print(f"Index: {es_config['index']}")
        print(f"Cluster: {es_config['cluster_url']}")
    else:
        logger.error("Failed to index document to Elasticsearch")
        sys.exit(1)

if __name__ == "__main__":
    main()
