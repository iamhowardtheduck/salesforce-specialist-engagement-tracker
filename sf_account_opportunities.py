#!/usr/bin/env python3
"""
Account-Specific Closed Opportunities Analysis

This script queries closed opportunities for specific accounts identified by their Salesforce URLs.
Perfect for analyzing performance of key accounts or account lists.

Usage:
    python sf_account_opportunities.py [options] <account_url1> [account_url2] [...]
    python sf_account_opportunities.py [options] --accounts-file accounts.txt
    
Examples:
    # Single account analysis
    python sf_account_opportunities.py "https://elastic.lightning.force.com/lightning/r/Account/001b000000kFpsaAAC/view"
    
    # Multiple accounts
    python sf_account_opportunities.py "https://elastic.lightning.force.com/lightning/r/Account/001b000000kFpsaAAC/view" "https://elastic.lightning.force.com/lightning/r/Account/001b000000kFpsaAAD/view"
    
    # From file
    python sf_account_opportunities.py --accounts-file key_accounts.txt
    
    # Only won opportunities from specific accounts
    python sf_account_opportunities.py --won-only "account_url"
"""

import sys
import json
import logging
import os
import argparse
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sf_auth import get_salesforce_connection
from config import get_elasticsearch_config, validate_es_config, get_elasticsearch_config_from_env
from elasticsearch import Elasticsearch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AccountOpportunitiesProcessor:
    """Process closed opportunities for specific accounts."""
    
    def __init__(self, es_config=None):
        self.sf = None
        self.es = None
        self.es_config = es_config
        
    def connect_salesforce(self) -> bool:
        """Connect to Salesforce."""
        try:
            self.sf = get_salesforce_connection()
            logger.info("Successfully connected to Salesforce")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {str(e)}")
            return False
    
    def connect_elasticsearch(self) -> bool:
        """Connect to Elasticsearch if config provided."""
        if not self.es_config:
            return False
            
        try:
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
            logger.info(f"Connected to Elasticsearch cluster: {info['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
            return False
    
    def extract_account_id(self, url: str) -> Optional[str]:
        """
        Extract Salesforce Account ID from URL.
        
        Args:
            url: Salesforce account URL
            
        Returns:
            Account ID or None if not found
        """
        # Pattern for Salesforce account ID (15 or 18 characters starting with 001)
        patterns = [
            r'/([A-Za-z0-9]{15,18})',  # Generic ID pattern
            r'/Account/([A-Za-z0-9]{15,18})',  # Explicit account pattern
            r'001[A-Za-z0-9]{12,15}',  # Account-specific pattern
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                account_id = match.group(1) if len(match.groups()) > 0 else match.group(0)
                # Ensure it starts with 001 (Account prefix)
                if account_id.startswith('001') and len(account_id) >= 15:
                    return account_id
        
        logger.error(f"Could not extract account ID from URL: {url}")
        return None
    
    def extract_account_ids_from_file(self, file_path: str) -> List[str]:
        """Extract account IDs from a file of URLs."""
        account_ids = []
        invalid_urls = []
        
        try:
            with open(file_path, 'r') as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            for url in urls:
                account_id = self.extract_account_id(url)
                if account_id:
                    account_ids.append(account_id)
                else:
                    invalid_urls.append(url)
            
            if invalid_urls:
                logger.warning(f"Could not extract account IDs from {len(invalid_urls)} URLs")
                for url in invalid_urls:
                    logger.warning(f"  Invalid: {url}")
            
            logger.info(f"Extracted {len(account_ids)} valid account IDs from file")
            return account_ids
            
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return []
    
    def get_account_info(self, account_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get account information for the provided IDs."""
        if not account_ids:
            return {}
        
        # Create comma-separated list for IN clause
        ids_str = "','".join(account_ids)
        
        query = f"""
        SELECT 
            Id, Name, Type, Industry, AnnualRevenue, 
            NumberOfEmployees, BillingCity, BillingState, BillingCountry,
            Owner.Name, CreatedDate, LastModifiedDate
        FROM Account 
        WHERE Id IN ('{ids_str}')
        """
        
        try:
            result = self.sf.query(query)
            
            account_info = {}
            for acc in result['records']:
                account_info[acc['Id']] = {
                    'name': acc['Name'],
                    'type': acc.get('Type'),
                    'industry': acc.get('Industry'),
                    'annual_revenue': acc.get('AnnualRevenue'),
                    'employees': acc.get('NumberOfEmployees'),
                    'city': acc.get('BillingCity'),
                    'state': acc.get('BillingState'),
                    'country': acc.get('BillingCountry'),
                    'owner': acc['Owner']['Name'] if acc.get('Owner') else None,
                    'created_date': acc['CreatedDate'],
                    'last_modified': acc['LastModifiedDate']
                }
            
            logger.info(f"Retrieved information for {len(account_info)} accounts")
            return account_info
            
        except Exception as e:
            logger.error(f"Error querying account information: {str(e)}")
            return {}
    
    def query_account_opportunities(self, account_ids: List[str], won_only=False, lost_only=False, 
                                  limit=None, date_from=None, date_to=None) -> List[Dict[str, Any]]:
        """Query closed opportunities for specific accounts."""
        
        if not account_ids:
            logger.error("No account IDs provided")
            return []
        
        # Create comma-separated list for IN clause
        ids_str = "','".join(account_ids)
        
        # Build SOQL query
        query = f"""
        SELECT 
            Id, Name, Account.Id, Account.Name, CloseDate, Amount, 
            StageName, IsWon, IsClosed, Type, Probability,
            CreatedDate, LastModifiedDate, Owner.Name, Owner.Id,
            Description, LeadSource, ForecastCategoryName
        FROM Opportunity 
        WHERE IsClosed = true AND AccountId IN ('{ids_str}')
        """
        
        # Add filters
        if won_only:
            query += " AND IsWon = true"
        elif lost_only:
            query += " AND IsWon = false"
        
        if date_from:
            query += f" AND CloseDate >= {date_from}"
        if date_to:
            query += f" AND CloseDate <= {date_to}"
        
        # Add ordering
        query += " ORDER BY Account.Name, CloseDate DESC, Amount DESC"
        
        # Add limit
        if limit:
            query += f" LIMIT {limit}"
        
        logger.info(f"Querying opportunities for {len(account_ids)} accounts...")
        logger.debug(f"SOQL Query: {query}")
        
        try:
            result = self.sf.query_all(query)
            
            logger.info(f"Retrieved {result['totalSize']} opportunities from Salesforce")
            
            opportunities = []
            for opp in result['records']:
                # Clean and format the data
                data = {
                    'opportunity_id': opp['Id'],
                    'opportunity_name': opp['Name'],
                    'account_id': opp['Account']['Id'] if opp.get('Account') else None,
                    'account_name': opp['Account']['Name'] if opp.get('Account') else None,
                    'close_date': opp['CloseDate'],
                    'amount': opp['Amount'] or 0,
                    'stage_name': opp['StageName'],
                    'is_won': opp['IsWon'],
                    'is_closed': opp['IsClosed'],
                    'type': opp['Type'],
                    'probability': opp['Probability'],
                    'created_date': opp['CreatedDate'],
                    'last_modified_date': opp['LastModifiedDate'],
                    'owner_name': opp['Owner']['Name'] if opp.get('Owner') else None,
                    'owner_id': opp['Owner']['Id'] if opp.get('Owner') else None,
                    'description': opp['Description'],
                    'lead_source': opp['LeadSource'],
                    'forecast_category': opp['ForecastCategoryName'],
                    'extracted_at': datetime.utcnow().isoformat(),
                    'source': 'salesforce_account_opportunities'
                }
                opportunities.append(data)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Error querying opportunities: {str(e)}")
            return []
    
    def analyze_by_account(self, opportunities: List[Dict[str, Any]], account_info: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze opportunities grouped by account."""
        
        # Group by account
        by_account = {}
        total_stats = {
            'total_opportunities': len(opportunities),
            'total_amount': 0,
            'won_opportunities': 0,
            'won_amount': 0,
            'lost_opportunities': 0,
            'lost_amount': 0
        }
        
        for opp in opportunities:
            account_id = opp['account_id']
            account_name = opp['account_name']
            
            if account_id not in by_account:
                by_account[account_id] = {
                    'account_info': account_info.get(account_id, {}),
                    'account_name': account_name,
                    'opportunities': [],
                    'stats': {
                        'total_count': 0,
                        'won_count': 0,
                        'lost_count': 0,
                        'total_amount': 0,
                        'won_amount': 0,
                        'lost_amount': 0,
                        'win_rate': 0,
                        'avg_deal_size': 0
                    }
                }
            
            by_account[account_id]['opportunities'].append(opp)
            by_account[account_id]['stats']['total_count'] += 1
            by_account[account_id]['stats']['total_amount'] += opp['amount']
            
            total_stats['total_amount'] += opp['amount']
            
            if opp['is_won']:
                by_account[account_id]['stats']['won_count'] += 1
                by_account[account_id]['stats']['won_amount'] += opp['amount']
                total_stats['won_opportunities'] += 1
                total_stats['won_amount'] += opp['amount']
            else:
                by_account[account_id]['stats']['lost_count'] += 1
                by_account[account_id]['stats']['lost_amount'] += opp['amount']
                total_stats['lost_opportunities'] += 1
                total_stats['lost_amount'] += opp['amount']
        
        # Calculate derived stats for each account
        for account_id, data in by_account.items():
            stats = data['stats']
            if stats['total_count'] > 0:
                stats['win_rate'] = (stats['won_count'] / stats['total_count']) * 100
                stats['avg_deal_size'] = stats['total_amount'] / stats['total_count']
        
        # Calculate total win rate
        if total_stats['total_opportunities'] > 0:
            total_stats['win_rate'] = (total_stats['won_opportunities'] / total_stats['total_opportunities']) * 100
            total_stats['avg_deal_size'] = total_stats['total_amount'] / total_stats['total_opportunities']
        
        return {
            'total_stats': total_stats,
            'by_account': by_account,
            'account_count': len(by_account)
        }
    
    def display_analysis(self, analysis: Dict[str, Any]):
        """Display the account opportunities analysis."""
        
        print(f"\n🎯 ACCOUNT OPPORTUNITIES ANALYSIS")
        print("=" * 60)
        
        total = analysis['total_stats']
        print(f"\n📊 Overall Statistics:")
        print(f"   Accounts Analyzed: {analysis['account_count']}")
        print(f"   Total Opportunities: {total['total_opportunities']:,}")
        print(f"   Won: {total['won_opportunities']:,} ({total.get('win_rate', 0):.1f}%)")
        print(f"   Lost: {total['lost_opportunities']:,}")
        print(f"   Total Revenue: ${total['total_amount']:,.2f}")
        print(f"   Won Revenue: ${total['won_amount']:,.2f}")
        if total.get('avg_deal_size'):
            print(f"   Average Deal: ${total['avg_deal_size']:,.2f}")
        
        # Account-by-account breakdown
        print(f"\n💼 BREAKDOWN BY ACCOUNT:")
        print("=" * 60)
        
        # Sort accounts by total revenue
        sorted_accounts = sorted(
            analysis['by_account'].items(),
            key=lambda x: x[1]['stats']['total_amount'],
            reverse=True
        )
        
        for i, (account_id, data) in enumerate(sorted_accounts, 1):
            stats = data['stats']
            account_info = data['account_info']
            
            print(f"\n{i}. {data['account_name']}")
            print(f"    Account ID: {account_id}")
            
            if account_info:
                if account_info.get('industry'):
                    print(f"    Industry: {account_info['industry']}")
                if account_info.get('annual_revenue'):
                    print(f"    Annual Revenue: ${account_info['annual_revenue']:,.2f}")
                if account_info.get('employees'):
                    print(f"    Employees: {account_info['employees']:,}")
                location_parts = []
                if account_info.get('city'):
                    location_parts.append(account_info['city'])
                if account_info.get('state'):
                    location_parts.append(account_info['state'])
                if account_info.get('country'):
                    location_parts.append(account_info['country'])
                if location_parts:
                    print(f"    Location: {', '.join(location_parts)}")
            
            print(f"    Opportunities: {stats['total_count']} (W:{stats['won_count']}, L:{stats['lost_count']})")
            print(f"    Win Rate: {stats['win_rate']:.1f}%")
            print(f"    Total Revenue: ${stats['total_amount']:,.2f}")
            print(f"    Won Revenue: ${stats['won_amount']:,.2f}")
            print(f"    Average Deal: ${stats['avg_deal_size']:,.2f}")
            
            # Show top deals for this account
            top_deals = sorted(data['opportunities'], key=lambda x: x['amount'], reverse=True)[:3]
            if top_deals:
                print(f"    Top Deals:")
                for j, deal in enumerate(top_deals, 1):
                    status = "WON" if deal['is_won'] else "LOST"
                    print(f"      {j}. ${deal['amount']:,.2f} - {deal['opportunity_name']} [{status}]")

def parse_date(date_str: str) -> str:
    """Parse date string and return in Salesforce format."""
    try:
        # Parse various date formats and convert to YYYY-MM-DD
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        return date_obj.isoformat()
    except ValueError:
        try:
            date_obj = datetime.strptime(date_str, '%m/%d/%Y').date()
            return date_obj.isoformat()
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD or MM/DD/YYYY")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Query closed opportunities for specific accounts')
    
    parser.add_argument('account_urls', nargs='*', help='Account URLs')
    parser.add_argument('--accounts-file', metavar='FILE', 
                       help='File containing account URLs (one per line)')
    parser.add_argument('--json-only', action='store_true', 
                       help='Output JSON only (no Elasticsearch)')
    parser.add_argument('--won-only', action='store_true',
                       help='Only closed won opportunities')
    parser.add_argument('--lost-only', action='store_true',
                       help='Only closed lost opportunities')
    parser.add_argument('--limit', type=int, metavar='N',
                       help='Limit results to N opportunities')
    parser.add_argument('--output-file', metavar='FILE',
                       help='Save JSON to specific file')
    parser.add_argument('--date-from', metavar='YYYY-MM-DD',
                       help='Only opportunities closed after this date')
    parser.add_argument('--date-to', metavar='YYYY-MM-DD',
                       help='Only opportunities closed before this date')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get account URLs/IDs
    account_urls = args.account_urls or []
    if args.accounts_file:
        if not os.path.exists(args.accounts_file):
            print(f"Error: File '{args.accounts_file}' does not exist.")
            sys.exit(1)
    
    if not account_urls and not args.accounts_file:
        parser.print_help()
        print(f"\nError: Must provide account URLs or --accounts-file")
        sys.exit(1)
    
    # Validate date arguments
    date_from = None
    date_to = None
    
    if args.date_from:
        try:
            date_from = parse_date(args.date_from)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    
    if args.date_to:
        try:
            date_to = parse_date(args.date_to)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    
    # Validate conflicting arguments
    if args.won_only and args.lost_only:
        print("Error: Cannot specify both --won-only and --lost-only")
        sys.exit(1)
    
    print("🏢 Account-Specific Closed Opportunities Analysis")
    print("=" * 55)
    
    # Initialize processor
    es_config = None
    if not args.json_only:
        try:
            es_config = get_elasticsearch_config_from_env()
            is_valid, error_msg = validate_es_config(es_config)
            
            if not is_valid:
                print("⚠️  Elasticsearch config invalid, switching to JSON-only mode")
                print(f"   Reason: {error_msg}")
                args.json_only = True
                
        except Exception:
            print("⚠️  No Elasticsearch config found, using JSON-only mode")
            args.json_only = True
    
    processor = AccountOpportunitiesProcessor(es_config)
    
    # Connect to Salesforce
    if not processor.connect_salesforce():
        print("❌ Failed to connect to Salesforce")
        sys.exit(1)
    
    # Extract account IDs
    account_ids = []
    
    # From command line URLs
    for url in account_urls:
        account_id = processor.extract_account_id(url)
        if account_id:
            account_ids.append(account_id)
        else:
            print(f"⚠️  Invalid account URL: {url}")
    
    # From file
    if args.accounts_file:
        file_ids = processor.extract_account_ids_from_file(args.accounts_file)
        account_ids.extend(file_ids)
    
    # Remove duplicates
    account_ids = list(set(account_ids))
    
    if not account_ids:
        print("❌ No valid account IDs found")
        sys.exit(1)
    
    print(f"🔍 Analyzing {len(account_ids)} account(s)")
    
    # Get account information
    account_info = processor.get_account_info(account_ids)
    
    # Query opportunities
    opportunities = processor.query_account_opportunities(
        account_ids,
        won_only=args.won_only,
        lost_only=args.lost_only,
        limit=args.limit,
        date_from=date_from,
        date_to=date_to
    )
    
    if not opportunities:
        print("❌ No opportunities found for the specified accounts")
        sys.exit(1)
    
    # Analyze data
    analysis = processor.analyze_by_account(opportunities, account_info)
    
    # Display analysis
    processor.display_analysis(analysis)
    
    # Save to JSON
    if args.output_file or args.json_only:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = args.output_file or f"account_opportunities_{timestamp}.json"
        
        output_data = {
            'analysis': analysis,
            'opportunities': opportunities,
            'account_info': account_info,
            'parameters': {
                'account_ids': account_ids,
                'won_only': args.won_only,
                'lost_only': args.lost_only,
                'date_from': date_from,
                'date_to': date_to,
                'limit': args.limit
            },
            'generated_at': datetime.utcnow().isoformat()
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\n💾 Data saved to: {filename}")

if __name__ == "__main__":
    main()
