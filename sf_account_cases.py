#!/usr/bin/env python3
"""
Account Cases Analysis with Elasticsearch Integration

Complete analysis of Salesforce cases for specific accounts with Elasticsearch indexing.
Includes case comments, advanced filtering, and comprehensive analytics.

Usage:
    python3 sf_account_cases.py <account_url>
    python3 sf_account_cases.py <account_url1> <account_url2> <account_url3>
    python3 sf_account_cases.py --accounts-file accounts.txt

Examples:
    python3 sf_account_cases.py "https://elastic.lightning.force.com/lightning/r/Account/001b000000kFpsaAAC/view"
    python3 sf_account_cases.py --accounts-file key_accounts.txt --priority High
    python3 sf_account_cases.py "account_url" --json-only --output cases_analysis.json
"""

import sys
import json
import re
import os
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sf_auth import get_salesforce_connection
from config import get_elasticsearch_config_from_env, validate_es_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AccountCasesProcessor:
    """Handles account cases analysis and Elasticsearch integration."""
    
    def __init__(self, es_config: Optional[Dict[str, Any]] = None):
        """Initialize the processor."""
        self.sf = None
        self.es = None
        self.es_config = es_config
    
    def connect_salesforce(self) -> bool:
        """Connect to Salesforce."""
        try:
            self.sf = get_salesforce_connection()
            logger.info("Connected to Salesforce successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {str(e)}")
            return False
    
    def connect_elasticsearch(self) -> bool:
        """Connect to Elasticsearch if config provided."""
        if not self.es_config:
            return False
        
        try:
            from elasticsearch import Elasticsearch
            
            connection_params = {
                'verify_certs': False,
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
        """Extract Salesforce Account ID from URL."""
        patterns = [
            r'/([A-Za-z0-9]{15,18})',
            r'/Account/([A-Za-z0-9]{15,18})',
            r'001[A-Za-z0-9]{12,15}',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                account_id = match.group(1) if len(match.groups()) > 0 else match.group(0)
                if account_id.startswith('001') and len(account_id) >= 15:
                    return account_id
        
        # Try as raw ID
        if url.startswith('001') and 15 <= len(url) <= 18:
            return url
        
        return None
    
    def get_account_info(self, account_ids: List[str]) -> Dict[str, Any]:
        """Get basic account information."""
        
        if not account_ids:
            return {}
        
        account_ids_str = "','".join(account_ids)
        
        try:
            query = f"""
            SELECT Id, Name, Type, Industry, AnnualRevenue, NumberOfEmployees,
                   BillingCity, BillingState, BillingCountry, Owner.Name
            FROM Account 
            WHERE Id IN ('{account_ids_str}')
            ORDER BY Name
            """
            
            result = self.sf.query(query)
            
            account_info = {}
            for record in result['records']:
                account_info[record['Id']] = {
                    'name': record['Name'],
                    'type': record.get('Type'),
                    'industry': record.get('Industry'),
                    'annual_revenue': record.get('AnnualRevenue'),
                    'employees': record.get('NumberOfEmployees'),
                    'city': record.get('BillingCity'),
                    'state': record.get('BillingState'),
                    'country': record.get('BillingCountry'),
                    'owner': record['Owner']['Name'] if record.get('Owner') else None
                }
            
            return account_info
            
        except Exception as e:
            logger.error(f"Error retrieving account info: {str(e)}")
            return {}
    
    def get_cases_for_accounts(self, account_ids: List[str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get all cases for the specified accounts."""
        
        if not account_ids:
            return []
        
        account_ids_str = "','".join(account_ids)
        
        # Build WHERE clause
        where_clauses = [f"AccountId IN ('{account_ids_str}')"]
        
        if filters.get('open_only'):
            where_clauses.append("IsClosed = false")
        elif filters.get('closed_only'):
            where_clauses.append("IsClosed = true")
        
        if filters.get('priority'):
            where_clauses.append(f"Priority = '{filters['priority']}'")
        
        if filters.get('status'):
            where_clauses.append(f"Status = '{filters['status']}'")
        
        if filters.get('type'):
            where_clauses.append(f"Type = '{filters['type']}'")
        
        if filters.get('date_from'):
            where_clauses.append(f"CreatedDate >= {filters['date_from']}")
        
        if filters.get('date_to'):
            where_clauses.append(f"CreatedDate <= {filters['date_to']}")
        
        where_clause = " AND ".join(where_clauses)
        
        try:
            query = f"""
            SELECT 
                Id, CaseNumber, Subject, Description, Status, Priority, Type, Origin,
                AccountId, Account.Name, ContactId, Contact.Name, Contact.Email,
                CreatedDate, ClosedDate, IsClosed, 
                Owner.Name, Owner.Id, Owner.Email,
                LastModifiedDate, LastModifiedBy.Name,
                Reason, IsDeleted, SuppliedEmail, SuppliedName
            FROM Case 
            WHERE {where_clause}
            ORDER BY Account.Name, CreatedDate DESC
            """
            
            if filters.get('limit'):
                query += f" LIMIT {filters['limit']}"
            
            logger.info(f"Querying cases with filters...")
            result = self.sf.query_all(query)
            
            logger.info(f"Found {result['totalSize']} cases")
            return result['records']
            
        except Exception as e:
            logger.error(f"Error querying cases: {str(e)}")
            return []
    
    def get_case_comments(self, case_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Get comments for the specified cases."""
        
        if not case_ids:
            return {}
        
        # Limit to prevent too large queries
        if len(case_ids) > 100:
            logger.warning(f"Limiting case comments query to first 100 cases")
            case_ids = case_ids[:100]
        
        case_ids_str = "','".join(case_ids)
        
        try:
            query = f"""
            SELECT 
                Id, ParentId, CommentBody, IsPublished, 
                CreatedDate, CreatedBy.Name, CreatedBy.Email,
                LastModifiedDate, LastModifiedBy.Name
            FROM CaseComment 
            WHERE ParentId IN ('{case_ids_str}')
            ORDER BY ParentId, CreatedDate ASC
            """
            
            result = self.sf.query_all(query)
            
            # Group comments by case
            comments_by_case = defaultdict(list)
            for comment in result['records']:
                case_id = comment['ParentId']
                comments_by_case[case_id].append({
                    'id': comment['Id'],
                    'body': comment['CommentBody'],
                    'is_published': comment['IsPublished'],
                    'created_date': comment['CreatedDate'],
                    'created_by': comment['CreatedBy']['Name'] if comment.get('CreatedBy') else None,
                    'created_by_email': comment['CreatedBy']['Email'] if comment.get('CreatedBy') else None,
                    'modified_date': comment['LastModifiedDate'],
                    'modified_by': comment['LastModifiedBy']['Name'] if comment.get('LastModifiedBy') else None
                })
            
            logger.info(f"Retrieved comments for {len(comments_by_case)} cases")
            return dict(comments_by_case)
            
        except Exception as e:
            logger.error(f"Error retrieving case comments: {str(e)}")
            return {}
    
    def process_cases_for_elasticsearch(self, cases: List[Dict[str, Any]], 
                                      case_comments: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Process cases data for Elasticsearch indexing."""
        
        processed_cases = []
        
        for case in cases:
            try:
                # Get case comments
                comments = case_comments.get(case['Id'], [])
                
                # Process case data
                data = {
                    'case_id': case['Id'],
                    'case_number': case['CaseNumber'],
                    'subject': case['Subject'],
                    'description': case['Description'],
                    'status': case['Status'],
                    'priority': case['Priority'],
                    'type': case['Type'],
                    'origin': case['Origin'],
                    'reason': case['Reason'],
                    'is_closed': case['IsClosed'],
                    'created_date': case['CreatedDate'],
                    'closed_date': case['ClosedDate'],
                    'last_modified_date': case['LastModifiedDate'],
                    
                    # Account information
                    'account_id': case['AccountId'],
                    'account_name': case['Account']['Name'] if case.get('Account') else None,
                    
                    # Contact information
                    'contact_id': case['ContactId'],
                    'contact_name': case['Contact']['Name'] if case.get('Contact') else None,
                    'contact_email': case['Contact']['Email'] if case.get('Contact') else None,
                    'supplied_email': case.get('SuppliedEmail'),
                    'supplied_name': case.get('SuppliedName'),
                    
                    # Owner information
                    'owner_id': case['Owner']['Id'] if case.get('Owner') else None,
                    'owner_name': case['Owner']['Name'] if case.get('Owner') else None,
                    'owner_email': case['Owner']['Email'] if case.get('Owner') else None,
                    
                    # Modified by
                    'last_modified_by': case['LastModifiedBy']['Name'] if case.get('LastModifiedBy') else None,
                    
                    # Comments
                    'comment_count': len(comments),
                    'comments': comments,
                    
                    # Metadata
                    'extracted_at': datetime.utcnow().isoformat(),
                    'source': 'salesforce_account_cases'
                }
                
                # Calculate case age
                created = datetime.fromisoformat(case['CreatedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                if case['IsClosed'] and case['ClosedDate']:
                    closed = datetime.fromisoformat(case['ClosedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                    data['resolution_time_days'] = (closed - created).days
                    data['case_age_days'] = (closed - created).days
                else:
                    data['case_age_days'] = (datetime.utcnow() - created).days
                    data['resolution_time_days'] = None
                
                processed_cases.append(data)
                
            except Exception as e:
                logger.error(f"Error processing case {case.get('Id', 'Unknown')}: {str(e)}")
                continue
        
        return processed_cases
    
    def index_to_elasticsearch(self, cases: List[Dict[str, Any]]) -> bool:
        """Index cases to Elasticsearch."""
        if not self.es:
            logger.error("No Elasticsearch connection available")
            return False
        
        try:
            from elasticsearch.helpers import bulk
            
            # Create index if it doesn't exist
            index_name = self.es_config['index']
            if not self.es.indices.exists(index=index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "case_id": {"type": "keyword"},
                            "case_number": {"type": "keyword"},
                            "subject": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "description": {"type": "text"},
                            "status": {"type": "keyword"},
                            "priority": {"type": "keyword"},
                            "type": {"type": "keyword"},
                            "origin": {"type": "keyword"},
                            "reason": {"type": "keyword"},
                            "is_closed": {"type": "boolean"},
                            "created_date": {"type": "date"},
                            "closed_date": {"type": "date"},
                            "last_modified_date": {"type": "date"},
                            "resolution_time_days": {"type": "integer"},
                            "case_age_days": {"type": "integer"},
                            "account_id": {"type": "keyword"},
                            "account_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "contact_id": {"type": "keyword"},
                            "contact_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "contact_email": {"type": "keyword"},
                            "supplied_email": {"type": "keyword"},
                            "supplied_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "owner_id": {"type": "keyword"},
                            "owner_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "owner_email": {"type": "keyword"},
                            "last_modified_by": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "comment_count": {"type": "integer"},
                            "comments": {
                                "type": "nested",
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "body": {"type": "text"},
                                    "is_published": {"type": "boolean"},
                                    "created_date": {"type": "date"},
                                    "created_by": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                    "created_by_email": {"type": "keyword"}
                                }
                            },
                            "extracted_at": {"type": "date"},
                            "source": {"type": "keyword"}
                        }
                    }
                }
                self.es.indices.create(index=index_name, body=mapping)
                logger.info(f"Created index '{index_name}' with mapping")
            
            # Prepare documents for bulk indexing
            actions = []
            for case in cases:
                action = {
                    '_index': index_name,
                    '_id': case['case_id'],  # Use case ID as document ID
                    '_source': case
                }
                actions.append(action)
            
            # Perform bulk indexing
            success, failed = bulk(self.es, actions, index=index_name)
            
            logger.info(f"Elasticsearch indexing: {success} successful, {len(failed)} failed")
            
            if failed:
                for failure in failed:
                    logger.error(f"Failed to index: {failure}")
            
            return len(failed) == 0
            
        except Exception as e:
            logger.error(f"Error indexing to Elasticsearch: {str(e)}")
            return False
    
    def analyze_cases(self, cases: List[Dict[str, Any]], account_info: Dict[str, Any], 
                     case_comments: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Analyze cases data and generate comprehensive statistics."""
        
        if not cases:
            return {
                'total_cases': 0,
                'by_account': {},
                'overall_stats': {
                    'total': 0,
                    'open': 0,
                    'closed': 0,
                    'avg_age_days': 0,
                    'total_comments': 0
                }
            }
        
        # Overall statistics
        total_cases = len(cases)
        open_cases = sum(1 for case in cases if not case['IsClosed'])
        closed_cases = total_cases - open_cases
        total_comments = sum(len(comments) for comments in case_comments.values())
        
        # Calculate average age and resolution time
        now = datetime.utcnow()
        ages = []
        resolution_times = []
        
        for case in cases:
            created = datetime.fromisoformat(case['CreatedDate'].replace('Z', '+00:00').replace('+00:00', ''))
            if case['IsClosed'] and case['ClosedDate']:
                closed = datetime.fromisoformat(case['ClosedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                age_days = (closed - created).days
                resolution_times.append(age_days)
            else:
                age_days = (now - created).days
            ages.append(age_days)
        
        avg_age = sum(ages) / len(ages) if ages else 0
        avg_resolution = sum(resolution_times) / len(resolution_times) if resolution_times else 0
        
        # Count by various dimensions
        priority_counts = Counter(case.get('Priority', 'None') for case in cases)
        status_counts = Counter(case.get('Status', 'None') for case in cases)
        type_counts = Counter(case.get('Type', 'None') for case in cases)
        origin_counts = Counter(case.get('Origin', 'None') for case in cases)
        
        # Group by account
        by_account = defaultdict(lambda: {
            'account_name': '',
            'account_info': {},
            'cases': [],
            'stats': {
                'total': 0,
                'open': 0,
                'closed': 0,
                'avg_age_days': 0,
                'avg_resolution_days': 0,
                'comments': 0,
                'priorities': {},
                'statuses': {},
                'types': {}
            }
        })
        
        for case in cases:
            account_id = case['AccountId']
            account_name = case['Account']['Name'] if case.get('Account') else 'Unknown'
            
            by_account[account_id]['account_name'] = account_name
            by_account[account_id]['account_info'] = account_info.get(account_id, {})
            by_account[account_id]['cases'].append(case)
            
            # Update stats
            stats = by_account[account_id]['stats']
            stats['total'] += 1
            
            if case['IsClosed']:
                stats['closed'] += 1
            else:
                stats['open'] += 1
            
            # Count comments for this case
            case_comments_count = len(case_comments.get(case['Id'], []))
            stats['comments'] += case_comments_count
            
            # Count by priority, status, type
            priority = case.get('Priority', 'None')
            status = case.get('Status', 'None')
            case_type = case.get('Type', 'None')
            
            stats['priorities'][priority] = stats['priorities'].get(priority, 0) + 1
            stats['statuses'][status] = stats['statuses'].get(status, 0) + 1
            stats['types'][case_type] = stats['types'].get(case_type, 0) + 1
        
        # Calculate averages per account
        for account_id, data in by_account.items():
            account_ages = []
            account_resolutions = []
            
            for case in data['cases']:
                created = datetime.fromisoformat(case['CreatedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                if case['IsClosed'] and case['ClosedDate']:
                    closed = datetime.fromisoformat(case['ClosedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                    age_days = (closed - created).days
                    account_resolutions.append(age_days)
                else:
                    age_days = (now - created).days
                account_ages.append(age_days)
            
            data['stats']['avg_age_days'] = sum(account_ages) / len(account_ages) if account_ages else 0
            data['stats']['avg_resolution_days'] = sum(account_resolutions) / len(account_resolutions) if account_resolutions else 0
        
        return {
            'total_cases': total_cases,
            'account_count': len(by_account),
            'by_account': dict(by_account),
            'overall_stats': {
                'total': total_cases,
                'open': open_cases,
                'closed': closed_cases,
                'close_rate': (closed_cases / total_cases * 100) if total_cases > 0 else 0,
                'avg_age_days': avg_age,
                'avg_resolution_days': avg_resolution,
                'total_comments': total_comments,
                'avg_comments_per_case': total_comments / total_cases if total_cases > 0 else 0,
                'priority_breakdown': dict(priority_counts),
                'status_breakdown': dict(status_counts),
                'type_breakdown': dict(type_counts),
                'origin_breakdown': dict(origin_counts)
            }
        }
    
    def display_analysis(self, analysis: Dict[str, Any]):
        """Display the cases analysis."""
        
        print(f"\n🎯 ACCOUNT CASES ANALYSIS")
        print("=" * 35)
        
        stats = analysis['overall_stats']
        print(f"\n📊 Overall Statistics:")
        print(f"   Accounts Analyzed: {analysis['account_count']}")
        print(f"   Total Cases: {stats['total']:,}")
        print(f"   Open: {stats['open']:,}")
        print(f"   Closed: {stats['closed']:,}")
        print(f"   Close Rate: {stats['close_rate']:.1f}%")
        print(f"   Average Age: {stats['avg_age_days']:.1f} days")
        if stats['avg_resolution_days'] > 0:
            print(f"   Average Resolution Time: {stats['avg_resolution_days']:.1f} days")
        print(f"   Total Comments: {stats['total_comments']:,}")
        print(f"   Avg Comments/Case: {stats['avg_comments_per_case']:.1f}")
        
        # Show priority breakdown
        if stats['priority_breakdown']:
            print(f"\n📈 Priority Breakdown:")
            for priority, count in sorted(stats['priority_breakdown'].items(), key=lambda x: x[1], reverse=True):
                percentage = (count / stats['total'] * 100)
                print(f"   {priority}: {count:,} ({percentage:.1f}%)")
        
        # Show status breakdown
        if stats['status_breakdown']:
            print(f"\n📊 Status Breakdown:")
            for status, count in sorted(stats['status_breakdown'].items(), key=lambda x: x[1], reverse=True):
                percentage = (count / stats['total'] * 100)
                print(f"   {status}: {count:,} ({percentage:.1f}%)")
        
        if not analysis['by_account']:
            return
        
        # Sort accounts by total cases
        sorted_accounts = sorted(
            analysis['by_account'].items(),
            key=lambda x: x[1]['stats']['total'],
            reverse=True
        )
        
        print(f"\n💼 BREAKDOWN BY ACCOUNT:")
        print("=" * 35)
        
        for i, (account_id, data) in enumerate(sorted_accounts, 1):
            account_stats = data['stats']
            account_info = data['account_info']
            
            print(f"\n{i}. {data['account_name']}")
            print(f"    Account ID: {account_id}")
            
            if account_info:
                if account_info.get('industry'):
                    print(f"    Industry: {account_info['industry']}")
                if account_info.get('employees'):
                    print(f"    Employees: {account_info['employees']:,}")
                location_parts = []
                if account_info.get('city'):
                    location_parts.append(account_info['city'])
                if account_info.get('state'):
                    location_parts.append(account_info['state'])
                if location_parts:
                    print(f"    Location: {', '.join(location_parts)}")
            
            print(f"    Cases: {account_stats['total']} (Open: {account_stats['open']}, Closed: {account_stats['closed']})")
            close_rate = (account_stats['closed']/account_stats['total']*100) if account_stats['total'] > 0 else 0
            print(f"    Close Rate: {close_rate:.1f}%")
            print(f"    Avg Age: {account_stats['avg_age_days']:.1f} days")
            if account_stats['avg_resolution_days'] > 0:
                print(f"    Avg Resolution: {account_stats['avg_resolution_days']:.1f} days")
            print(f"    Comments: {account_stats['comments']}")
            
            # Show top priorities
            if account_stats['priorities']:
                top_priorities = sorted(account_stats['priorities'].items(), key=lambda x: x[1], reverse=True)
                print(f"    Top Priorities: {', '.join(f'{p}({c})' for p, c in top_priorities[:3])}")
            
            # Show recent cases
            recent_cases = sorted(data['cases'], key=lambda x: x['CreatedDate'], reverse=True)[:3]
            if recent_cases:
                print(f"    Recent Cases:")
                for j, case in enumerate(recent_cases, 1):
                    status = case['Status'] or 'No Status'
                    priority = case['Priority'] or 'No Priority'
                    created = case['CreatedDate'][:10] if case['CreatedDate'] else 'Unknown'
                    subject = case['Subject'][:40] + "..." if case['Subject'] and len(case['Subject']) > 40 else case['Subject']
                    print(f"      {j}. {case['CaseNumber']} - {subject}")
                    print(f"         {status} | {priority} | {created}")

def main():
    """Main function."""
    
    parser = argparse.ArgumentParser(description='Analyze Salesforce cases for specific accounts with Elasticsearch integration')
    
    # Account specification
    parser.add_argument('account_urls', nargs='*', help='Account URLs to analyze')
    parser.add_argument('--accounts-file', help='File containing account URLs (one per line)')
    
    # Filters
    parser.add_argument('--open-only', action='store_true', help='Only open cases')
    parser.add_argument('--closed-only', action='store_true', help='Only closed cases')
    parser.add_argument('--priority', choices=['High', 'Medium', 'Low'], help='Filter by priority')
    parser.add_argument('--status', help='Filter by status')
    parser.add_argument('--type', help='Filter by case type')
    parser.add_argument('--date-from', help='Filter cases created from date (YYYY-MM-DD)')
    parser.add_argument('--date-to', help='Filter cases created to date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, help='Limit number of cases returned')
    
    # Output options
    parser.add_argument('--json-only', action='store_true', help='Output JSON only (no Elasticsearch)')
    parser.add_argument('--output-file', help='Output JSON filename')
    parser.add_argument('--no-comments', action='store_true', help='Skip case comments retrieval')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get account URLs/IDs
    account_urls = args.account_urls or []
    if args.accounts_file:
        if not os.path.exists(args.accounts_file):
            print(f"Error: File '{args.accounts_file}' does not exist.")
            sys.exit(1)
        
        with open(args.accounts_file, 'r') as f:
            file_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            account_urls.extend(file_urls)
    
    if not account_urls:
        parser.print_help()
        print(f"\nError: Must provide account URLs or --accounts-file parameter")
        sys.exit(1)
    
    # Get Elasticsearch config
    es_config = None
    if not args.json_only:
        try:
            es_config = get_elasticsearch_config_from_env()
            if es_config:
                is_valid, error_msg = validate_es_config(es_config)
                if not is_valid:
                    print(f"⚠️  Elasticsearch config invalid: {error_msg}")
                    print("⚠️  Elasticsearch config invalid, switching to JSON-only mode")
                    args.json_only = True
            else:
                print("⚠️  No Elasticsearch config found, using JSON-only mode")
                args.json_only = True
        except Exception as e:
            print(f"⚠️  Error getting Elasticsearch config: {str(e)}")
            args.json_only = True
    
    processor = AccountCasesProcessor(es_config)
    
    # Connect to Salesforce
    if not processor.connect_salesforce():
        print("❌ Failed to connect to Salesforce")
        sys.exit(1)
    
    # Connect to Elasticsearch if needed
    if not args.json_only and es_config:
        print("🔍 Connecting to Elasticsearch...")
        if not processor.connect_elasticsearch():
            print("⚠️  Failed to connect to Elasticsearch, switching to JSON-only mode")
            args.json_only = True
    
    # Extract account IDs
    account_ids = []
    for url in account_urls:
        account_id = processor.extract_account_id(url)
        if account_id:
            account_ids.append(account_id)
        else:
            print(f"⚠️  Invalid account URL: {url}")
    
    if not account_ids:
        print(f"❌ No valid account IDs found")
        sys.exit(1)
    
    print(f"🔍 Analyzing {len(account_ids)} account(s)")
    
    # Get account information
    account_info = processor.get_account_info(account_ids)
    print(f"✅ Account information retrieved for {len(account_info)} accounts")
    
    # Build filters
    filters = {
        'open_only': args.open_only,
        'closed_only': args.closed_only,
        'priority': args.priority,
        'status': args.status,
        'type': args.type,
        'date_from': args.date_from,
        'date_to': args.date_to,
        'limit': args.limit
    }
    
    # Get cases
    cases = processor.get_cases_for_accounts(account_ids, filters)
    
    if not cases:
        print(f"📋 No cases found for the specified accounts and filters")
        return
    
    # Get case comments
    case_comments = {}
    if not args.no_comments and cases:
        case_ids = [case['Id'] for case in cases]
        case_comments = processor.get_case_comments(case_ids)
    
    # Analyze data
    analysis = processor.analyze_cases(cases, account_info, case_comments)
    
    # Display results
    processor.display_analysis(analysis)
    
    # Index to Elasticsearch if not JSON-only mode
    if not args.json_only and processor.es:
        print(f"\n🔍 Indexing to Elasticsearch...")
        # Process cases for ES
        es_cases = processor.process_cases_for_elasticsearch(cases, case_comments)
        if processor.index_to_elasticsearch(es_cases):
            print(f"✅ Successfully indexed {len(es_cases)} cases to Elasticsearch")
            print(f"   Index: {processor.es_config['index']}")
        else:
            print("⚠️  Some cases failed to index to Elasticsearch")
    
    # Save to JSON
    if args.output_file or args.json_only:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = args.output_file or f"account_cases_{timestamp}.json"
        
        output_data = {
            'analysis': analysis,
            'raw_data': {
                'cases': cases,
                'case_comments': case_comments,
                'account_info': account_info
            },
            'metadata': {
                'generated_at': datetime.utcnow().isoformat(),
                'total_cases': len(cases),
                'total_comments': sum(len(comments) for comments in case_comments.values()),
                'filters': filters
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\n💾 Data saved to: {filename}")

if __name__ == "__main__":
    main()
