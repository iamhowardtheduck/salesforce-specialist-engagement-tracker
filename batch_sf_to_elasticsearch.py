#!/usr/bin/env python3
"""
Batch Salesforce to Elasticsearch Integration Script

This script processes multiple Salesforce opportunities from a file and ingests them into Elasticsearch.

Usage:
    python batch_sf_to_elasticsearch.py <urls_file>
    
Where urls_file contains one opportunity URL per line.

Example urls_file content:
    https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view
    https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000YYYYYY/view
"""

import sys
import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path

# Add current directory to path for config import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from elasticsearch import Elasticsearch
from sf_auth import get_salesforce_connection
from config import get_elasticsearch_config, validate_es_config, get_elasticsearch_config_from_env

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'batch_{LOG_FILE}'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SalesforceBatchProcessor:
    """Batch processor for Salesforce to Elasticsearch operations."""
    
    def __init__(self, es_config=None):
        self.sf = None
        self.es = None
        self.es_config = es_config
        self.processed_count = 0
        self.failed_count = 0
        self.results = []
    
    def connect_services(self) -> bool:
        """Connect to Salesforce and Elasticsearch."""
        try:
            # Connect to Salesforce
            self.sf = get_salesforce_connection()
            logger.info("Successfully connected to Salesforce")
            
            # Get ES config if not provided
            if not self.es_config:
                self.es_config = get_elasticsearch_config()
                is_valid, error_msg = validate_es_config(self.es_config)
                if not is_valid:
                    logger.error(f"Invalid configuration: {error_msg}")
                    return False
            
            # Connect to Elasticsearch
            connection_params = {
                'verify_certs': self.es_config.get('verify_certs', False),
                'request_timeout': 30
            }
            
            if self.es_config.get('auth_type') == 'api_key':
                connection_params['api_key'] = self.es_config['api_key']
            else:
                connection_params['basic_auth'] = (self.es_config['username'], self.es_config['password'])
            
            self.es = Elasticsearch(
                [self.es_config['cluster_url']],
                **connection_params
            )
            
            # Test connection
            info = self.es.info()
            logger.info(f"Connected to Elasticsearch cluster: {info['name']}")
            
            # Create index if needed
            self._create_index_if_not_exists()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to services: {str(e)}")
            return False
    
    def _create_index_if_not_exists(self):
        """Create index with mapping if it doesn't exist."""
        index_name = self.es_config['index']
        
        if not self.es.indices.exists(index=index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "opportunity_id": {"type": "keyword"},
                        "opportunity_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "account_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "close_date": {"type": "date"},
                        "amount": {"type": "double"},
                        "tcv_amount": {"type": "double"},
                        "extracted_at": {"type": "date"},
                        "source": {"type": "keyword"}
                    }
                }
            }
            self.es.indices.create(index=index_name, body=mapping)
            logger.info(f"Created index '{index_name}' with mapping")
        else:
            logger.info(f"Index '{index_name}' already exists")
    
    def extract_opportunity_id(self, url: str) -> str:
        """Extract opportunity ID from Salesforce URL."""
        import re
        patterns = [
            r'/([A-Za-z0-9]{15,18})',
            r'/Opportunity/([A-Za-z0-9]{15,18})',
            r'006[A-Za-z0-9]{12,15}',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                opportunity_id = match.group(1) if len(match.groups()) > 0 else match.group(0)
                if opportunity_id.startswith('006') and len(opportunity_id) >= 15:
                    return opportunity_id
        return None
    
    def process_opportunities_bulk(self, opportunity_ids: List[str]) -> List[Dict[str, Any]]:
        """Query multiple opportunities in a single SOQL call."""
        if not opportunity_ids:
            return []
        
        # Create comma-separated list for IN clause
        ids_str = "','"join(opportunity_ids)
        
        soql_query = f"""
        SELECT 
            Id,
            Name,
            Account.Name,
            CloseDate,
            Amount,
            TCV__c
        FROM Opportunity 
        WHERE Id IN ('{ids_str}')
        """
        
        try:
            result = self.sf.query(soql_query)
            opportunities = []
            
            for opp in result['records']:
                data = {
                    'opportunity_id': opp['Id'],
                    'opportunity_name': opp['Name'],
                    'account_name': opp['Account']['Name'] if opp.get('Account') else None,
                    'close_date': opp['CloseDate'],
                    'amount': opp['Amount'],
                    'tcv_amount': opp.get('TCV__c'),
                    'extracted_at': datetime.utcnow().isoformat(),
                    'source': 'salesforce_batch'
                }
                opportunities.append(data)
            
            logger.info(f"Retrieved {len(opportunities)} opportunities from Salesforce")
            return opportunities
            
        except Exception as e:
            logger.error(f"Error in bulk query: {str(e)}")
            return []
    
    def bulk_index_documents(self, documents: List[Dict[str, Any]]) -> bool:
        """Bulk index documents to Elasticsearch."""
        if not documents:
            return True
        
        try:
            from elasticsearch.helpers import bulk
            
            # Prepare documents for bulk indexing
            actions = []
            for doc in documents:
                action = {
                    '_index': self.es_config['index'],
                    '_id': doc['opportunity_id'],
                    '_source': doc
                }
                actions.append(action)
            
            # Perform bulk indexing
            success, failed = bulk(self.es, actions, index=self.es_config['index'])
            
            logger.info(f"Bulk indexed {success} documents successfully")
            if failed:
                logger.warning(f"{len(failed)} documents failed to index")
            
            return len(failed) == 0
            
        except Exception as e:
            logger.error(f"Error in bulk indexing: {str(e)}")
            return False
    
    def process_urls_file(self, file_path: str) -> Dict[str, Any]:
        """Process URLs from a file."""
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            logger.info(f"Processing {len(urls)} URLs from {file_path}")
            
            # Extract opportunity IDs
            opportunity_ids = []
            invalid_urls = []
            
            for url in urls:
                opp_id = self.extract_opportunity_id(url)
                if opp_id:
                    opportunity_ids.append(opp_id)
                else:
                    invalid_urls.append(url)
                    logger.warning(f"Could not extract opportunity ID from: {url}")
            
            if invalid_urls:
                logger.warning(f"{len(invalid_urls)} URLs were invalid")
            
            # Process in batches of 100 (Salesforce SOQL limit)
            batch_size = 100
            all_documents = []
            
            for i in range(0, len(opportunity_ids), batch_size):
                batch_ids = opportunity_ids[i:i + batch_size]
                batch_docs = self.process_opportunities_bulk(batch_ids)
                all_documents.extend(batch_docs)
                
                logger.info(f"Processed batch {i//batch_size + 1}/{(len(opportunity_ids) + batch_size - 1)//batch_size}")
            
            # Bulk index all documents
            success = self.bulk_index_documents(all_documents)
            
            # Prepare results
            results = {
                'total_urls': len(urls),
                'valid_urls': len(opportunity_ids),
                'invalid_urls': len(invalid_urls),
                'processed_opportunities': len(all_documents),
                'indexed_successfully': success,
                'processing_time': datetime.utcnow().isoformat(),
                'invalid_url_list': invalid_urls
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return {'error': str(e)}
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate a processing report."""
        report = f"""
╔══════════════════════════════════════════════════════════════════════╗
║                    BATCH PROCESSING REPORT                          ║
╠══════════════════════════════════════════════════════════════════════╣
║ Total URLs processed: {results.get('total_urls', 0):>40} ║
║ Valid URLs: {results.get('valid_urls', 0):>55} ║
║ Invalid URLs: {results.get('invalid_urls', 0):>53} ║
║ Opportunities processed: {results.get('processed_opportunities', 0):>42} ║
║ Indexing successful: {results.get('indexed_successfully', False):>46} ║
║ Processing time: {results.get('processing_time', 'Unknown'):>50} ║
╚══════════════════════════════════════════════════════════════════════╝
"""
        
        if results.get('invalid_url_list'):
            report += "\nInvalid URLs:\n"
            for url in results['invalid_url_list']:
                report += f"  - {url}\n"
        
        return report

def main():
    """Main execution function."""
    if len(sys.argv) != 2:
        print("Usage: python batch_sf_to_elasticsearch.py <urls_file>")
        print("Example: python batch_sf_to_elasticsearch.py opportunity_urls.txt")
        sys.exit(1)
    
    urls_file = sys.argv[1]
    
    if not Path(urls_file).exists():
        print(f"Error: File '{urls_file}' does not exist.")
        sys.exit(1)
    
    logger.info(f"Starting batch processing of {urls_file}")
    
    # Get Elasticsearch configuration
    try:
        # Try environment variables first for non-interactive use
        es_config = get_elasticsearch_config_from_env()
        is_valid, error_msg = validate_es_config(es_config)
        
        if not is_valid:
            # Fall back to interactive configuration
            print("Environment variables not set or incomplete. Using interactive configuration.")
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
    
    processor = SalesforceBatchProcessor(es_config)
    
    # Connect to services
    if not processor.connect_services():
        logger.error("Failed to connect to required services")
        sys.exit(1)
    
    # Process the file
    results = processor.process_urls_file(urls_file)
    
    # Generate and display report
    if 'error' not in results:
        report = processor.generate_report(results)
        print(report)
        logger.info("Batch processing completed successfully")
        
        # Save detailed results
        results_file = f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Detailed results saved to {results_file}")
        
    else:
        logger.error(f"Batch processing failed: {results['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main()
