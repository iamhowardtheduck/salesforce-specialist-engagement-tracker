#!/usr/bin/env python3
"""
Interactive Salesforce to Elasticsearch Integration Tool

This script provides an interactive interface for processing Salesforce opportunities
and ingesting them into Elasticsearch.
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path
import logging

from elasticsearch import Elasticsearch
from sf_auth import get_salesforce_connection
from config import get_elasticsearch_config, validate_es_config

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('interactive_sf_to_es.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class InteractiveSFProcessor:
    """Interactive Salesforce to Elasticsearch processor."""
    
    def __init__(self):
        self.sf = None
        self.es = None
        self.es_config = None
        self.connected = False
    
    def display_banner(self):
        """Display welcome banner."""
        banner = """
╔══════════════════════════════════════════════════════════════════════╗
║                Salesforce to Elasticsearch Integration              ║
║                          Interactive Tool                           ║
╠══════════════════════════════════════════════════════════════════════╣
║ This tool helps you extract opportunity data from Salesforce        ║
║ and index it into your Elasticsearch cluster.                       ║
╚══════════════════════════════════════════════════════════════════════╝
"""
        print(banner)
    
    def test_connections(self):
        """Test connections to Salesforce and Elasticsearch."""
        print("\n🔗 Testing connections...")
        
        # Get Elasticsearch configuration first
        try:
            print("  ├─ Getting Elasticsearch configuration...")
            self.es_config = get_elasticsearch_config()
            is_valid, error_msg = validate_es_config(self.es_config)
            
            if not is_valid:
                print(f"  ❌ Configuration invalid: {error_msg}")
                return False
                
            print("  ✓ Configuration valid")
        except KeyboardInterrupt:
            print("\n  ❌ Configuration cancelled by user")
            return False
        except Exception as e:
            print(f"  ❌ Configuration error: {str(e)}")
            return False
        
        # Test Salesforce
        try:
            print("  ├─ Connecting to Salesforce...", end=" ")
            self.sf = get_salesforce_connection()
            print("✓ Connected")
        except Exception as e:
            print("✗ Failed")
            print(f"     Error: {str(e)}")
            return False
        
        # Test Elasticsearch
        try:
            print("  ├─ Connecting to Elasticsearch...", end=" ")
            
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
            
            info = self.es.info()
            print(f"✓ Connected to {info['name']}")
        except Exception as e:
            print("✗ Failed")
            print(f"     Error: {str(e)}")
            return False
        
        # Check/create index
        try:
            print("  └─ Verifying index...", end=" ")
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
                print(f"✓ Created '{index_name}'")
            else:
                print(f"✓ Index '{index_name}' exists")
        except Exception as e:
            print("✗ Failed")
            print(f"     Error: {str(e)}")
            return False
        
        self.connected = True
        return True
    
    def display_menu(self):
        """Display main menu."""
        menu = """
📋 Choose an option:
   1. Process a single opportunity URL
   2. Process multiple URLs from file
   3. Test opportunity ID extraction
   4. View current configuration
   5. Check index status
   6. Exit
"""
        print(menu)
    
    def extract_opportunity_id(self, url: str) -> str:
        """Extract opportunity ID from URL."""
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
    
    def process_single_url(self):
        """Process a single opportunity URL."""
        print("\n📝 Enter opportunity URL:")
        url = input("URL: ").strip()
        
        if not url:
            print("❌ No URL provided")
            return
        
        # Extract opportunity ID
        opp_id = self.extract_opportunity_id(url)
        if not opp_id:
            print("❌ Could not extract opportunity ID from URL")
            return
        
        print(f"✓ Extracted opportunity ID: {opp_id}")
        
        # Query Salesforce
        try:
            print("📡 Querying Salesforce...")
            soql_query = f"""
            SELECT 
                Id,
                Name,
                Account.Name,
                CloseDate,
                Amount,
                TCV__c
            FROM Opportunity 
            WHERE Id = '{opp_id}'
            """
            
            result = self.sf.query(soql_query)
            
            if result['totalSize'] == 0:
                print("❌ No opportunity found with this ID")
                return
            
            opp = result['records'][0]
            
            # Prepare data
            data = {
                'opportunity_id': opp['Id'],
                'opportunity_name': opp['Name'],
                'account_name': opp['Account']['Name'] if opp.get('Account') else None,
                'close_date': opp['CloseDate'],
                'amount': opp['Amount'],
                'tcv_amount': opp.get('TCV__c'),
                'extracted_at': datetime.utcnow().isoformat(),
                'source': 'salesforce_interactive'
            }
            
            # Display data
            print("\n📊 Opportunity Data:")
            for key, value in data.items():
                if key != 'extracted_at':
                    print(f"   {key.replace('_', ' ').title()}: {value}")
            
            # Confirm indexing
            confirm = input("\n❓ Index this opportunity to Elasticsearch? (y/N): ").strip().lower()
            
            if confirm == 'y':
                response = self.es.index(
                    index=self.es_config['index'],
                    id=data['opportunity_id'],
                    body=data
                )
                print(f"✅ Successfully indexed! Document ID: {response['_id']}")
            else:
                print("⏭️  Skipped indexing")
                
        except Exception as e:
            print(f"❌ Error processing opportunity: {str(e)}")
    
    def process_file_urls(self):
        """Process URLs from a file."""
        print("\n📁 Enter path to URLs file:")
        file_path = input("File path: ").strip()
        
        if not file_path or not Path(file_path).exists():
            print("❌ File not found")
            return
        
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            print(f"📋 Found {len(urls)} URLs in file")
            
            # Process URLs
            valid_ids = []
            for i, url in enumerate(urls, 1):
                opp_id = self.extract_opportunity_id(url)
                if opp_id:
                    valid_ids.append(opp_id)
                    print(f"  {i:3d}. ✓ {opp_id}")
                else:
                    print(f"  {i:3d}. ❌ Invalid URL")
            
            if not valid_ids:
                print("❌ No valid opportunity URLs found")
                return
            
            confirm = input(f"\n❓ Process {len(valid_ids)} opportunities? (y/N): ").strip().lower()
            
            if confirm == 'y':
                print("🚀 Starting batch processing...")
                
                # Import batch processor
                from batch_sf_to_elasticsearch import SalesforceBatchProcessor
                
                processor = SalesforceBatchProcessor(self.es_config)
                processor.sf = self.sf
                processor.es = self.es
                
                # Process in bulk
                documents = processor.process_opportunities_bulk(valid_ids)
                success = processor.bulk_index_documents(documents)
                
                if success:
                    print(f"✅ Successfully processed {len(documents)} opportunities")
                else:
                    print("⚠️  Some documents may have failed to index")
            
        except Exception as e:
            print(f"❌ Error processing file: {str(e)}")
    
    def test_url_extraction(self):
        """Test URL extraction functionality."""
        print("\n🧪 URL Extraction Tester")
        print("Enter Salesforce opportunity URLs to test ID extraction:")
        print("(Enter empty line to return to menu)")
        
        while True:
            url = input("\nURL: ").strip()
            if not url:
                break
                
            opp_id = self.extract_opportunity_id(url)
            if opp_id:
                print(f"✓ Extracted ID: {opp_id}")
            else:
                print("❌ Could not extract opportunity ID")
    
    def view_configuration(self):
        """Display current configuration."""
        if self.es_config:
            cluster_url = self.es_config.get('cluster_url', 'Not set')
            username = self.es_config.get('username', 'Not set')
            auth_type = self.es_config.get('auth_type', 'basic')
            index_name = self.es_config.get('index', 'Not set')
            
            auth_info = f"API Key ({auth_type})" if auth_type == 'api_key' else f"Username/Password ({username})"
        else:
            cluster_url = username = index_name = auth_info = "Not configured"
        
        config_info = f"""
🔧 Current Configuration:
   Elasticsearch:
     ├─ Cluster: {cluster_url}
     ├─ Authentication: {auth_info}
     ├─ SSL Verification: Disabled
     ├─ Index: {index_name}
     └─ Status: {"✓ Connected" if self.connected else "❌ Not connected"}
   
   Salesforce:
     ├─ Instance: https://elastic.my.salesforce.com
     └─ Status: {"✓ Connected" if self.connected else "❌ Not connected"}
   
   Logging:
     ├─ Level: INFO
     └─ File: interactive_sf_to_es.log
"""
        print(config_info)
    
    def check_index_status(self):
        """Check Elasticsearch index status."""
        if not self.connected or not self.es_config:
            print("❌ Not connected to Elasticsearch")
            return
        
        index_name = self.es_config['index']
        
        try:
            # Get index stats
            stats = self.es.indices.stats(index=index_name)
            doc_count = stats['indices'][index_name]['total']['docs']['count']
            size = stats['indices'][index_name]['total']['store']['size_in_bytes']
            
            # Get mapping
            mapping = self.es.indices.get_mapping(index=index_name)
            
            status_info = f"""
📊 Index Status: {index_name}
   ├─ Document count: {doc_count:,}
   ├─ Size: {size / 1024 / 1024:.2f} MB
   ├─ Mapping fields: {len(mapping[index_name]['mappings']['properties'])}
   └─ Health: {"✓ Good" if doc_count >= 0 else "❌ Error"}
"""
            print(status_info)
            
            if doc_count > 0:
                show_sample = input("❓ Show sample document? (y/N): ").strip().lower()
                if show_sample == 'y':
                    sample = self.es.search(index=index_name, size=1)
                    if sample['hits']['hits']:
                        print("\n📋 Sample document:")
                        doc = sample['hits']['hits'][0]['_source']
                        for key, value in doc.items():
                            print(f"   {key}: {value}")
            
        except Exception as e:
            print(f"❌ Error checking index status: {str(e)}")
    
    def run(self):
        """Run the interactive tool."""
        self.display_banner()
        
        # Test connections
        if not self.test_connections():
            print("\n❌ Cannot proceed without proper connections.")
            print("Please check your configuration and try again.")
            return
        
        print("\n✅ All systems connected successfully!")
        
        # Main loop
        while True:
            self.display_menu()
            
            try:
                choice = input("Enter your choice (1-6): ").strip()
                
                if choice == '1':
                    self.process_single_url()
                elif choice == '2':
                    self.process_file_urls()
                elif choice == '3':
                    self.test_url_extraction()
                elif choice == '4':
                    self.view_configuration()
                elif choice == '5':
                    self.check_index_status()
                elif choice == '6':
                    print("\n👋 Goodbye!")
                    break
                else:
                    print("❌ Invalid choice. Please select 1-6.")
                
                input("\nPress Enter to continue...")
                
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Unexpected error: {str(e)}")
                input("Press Enter to continue...")

def main():
    """Main execution function."""
    try:
        processor = InteractiveSFProcessor()
        processor.run()
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
