#!/usr/bin/env python3
"""
Salesforce Cases to Elasticsearch Integration

This script retrieves Salesforce cases (support tickets) with comments/notes
and indexes them to Elasticsearch for analysis.

Features:
- Retrieve all cases or filter by account
- Include case comments and notes
- Support both open and closed cases
- Full Elasticsearch integration
- JSON output option

Usage:
    python3 sf_cases_to_elasticsearch.py                    # All cases
    python3 sf_cases_to_elasticsearch.py --account-id ID    # Specific account
    python3 sf_cases_to_elasticsearch.py --open-only        # Open cases only
    python3 sf_cases_to_elasticsearch.py --closed-only      # Closed cases only
    python3 sf_cases_to_elasticsearch.py --json-only        # JSON output only
    python3 sf_cases_to_elasticsearch.py --with-comments    # Include case comments
"""

import sys
import os
import json
import logging
import argparse
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sf_auth import get_salesforce_connection
from config import get_elasticsearch_config, get_elasticsearch_config_from_env, validate_es_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SalesforceCasesProcessor:
    """Process Salesforce cases and index to Elasticsearch."""
    
    def __init__(self, es_config: Optional[Dict[str, Any]] = None):
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
            
            # Build connection parameters
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
    
    def get_cases(self, account_id: Optional[str] = None, open_only: bool = False, 
                  closed_only: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieve cases from Salesforce.
        
        Args:
            account_id: Optional account ID to filter by
            open_only: Only retrieve open cases
            closed_only: Only retrieve closed cases
            limit: Optional limit on number of cases
            
        Returns:
            List of case records
        """
        try:
            # Build SOQL query
            soql = """
            SELECT 
                Id, CaseNumber, Subject, Description, Status, Priority, Type,
                AccountId, Account.Name, ContactId, Contact.Name, Contact.Email,
                CreatedDate, ClosedDate, LastModifiedDate,
                Origin, Reason, SuppliedEmail, SuppliedName, SuppliedPhone,
                IsClosed, IsEscalated, EscalatedDate,
                Owner.Id, Owner.Name, Owner.Email,
                CreatedBy.Id, CreatedBy.Name,
                LastModifiedBy.Id, LastModifiedBy.Name,
                ParentId, Parent.CaseNumber,
                BusinessHoursId, SlaStartDate, SlaExitDate
            FROM Case
            """
            
            # Add WHERE conditions
            where_conditions = []
            
            if account_id:
                where_conditions.append(f"AccountId = '{account_id}'")
            
            if open_only:
                where_conditions.append("IsClosed = false")
            elif closed_only:
                where_conditions.append("IsClosed = true")
            
            if where_conditions:
                soql += " WHERE " + " AND ".join(where_conditions)
            
            # Add ordering
            soql += " ORDER BY CreatedDate DESC"
            
            # Add limit
            if limit:
                soql += f" LIMIT {limit}"
            
            logger.info(f"Executing SOQL: {soql}")
            
            # Execute query
            result = self.sf.query_all(soql)
            cases = result['records']
            
            logger.info(f"Retrieved {len(cases)} cases from Salesforce")
            return cases
            
        except Exception as e:
            logger.error(f"Error retrieving cases: {str(e)}")
            return []
    
    def get_case_comments(self, case_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Retrieve case comments for multiple cases.
        
        Args:
            case_ids: List of case IDs
            
        Returns:
            Dictionary mapping case ID to list of comments
        """
        if not case_ids:
            return {}
        
        try:
            # Build SOQL for case comments
            case_ids_str = "','".join(case_ids)
            soql = f"""
            SELECT 
                Id, ParentId, CommentBody, CreatedDate, LastModifiedDate,
                CreatedBy.Id, CreatedBy.Name, CreatedBy.Email,
                IsPublished, IsDeleted
            FROM CaseComment
            WHERE ParentId IN ('{case_ids_str}')
            AND IsDeleted = false
            ORDER BY ParentId, CreatedDate ASC
            """
            
            logger.info(f"Retrieving comments for {len(case_ids)} cases")
            result = self.sf.query_all(soql)
            
            # Group comments by case ID
            comments_by_case = {}
            for comment in result['records']:
                case_id = comment['ParentId']
                if case_id not in comments_by_case:
                    comments_by_case[case_id] = []
                comments_by_case[case_id].append(comment)
            
            logger.info(f"Retrieved {len(result['records'])} comments total")
            return comments_by_case
            
        except Exception as e:
            logger.error(f"Error retrieving case comments: {str(e)}")
            return {}
    
    def process_cases_for_elasticsearch(self, cases: List[Dict[str, Any]], 
                                      comments_by_case: Dict[str, List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Process cases for Elasticsearch indexing.
        
        Args:
            cases: List of case records from Salesforce
            comments_by_case: Optional dictionary of case comments
            
        Returns:
            List of processed documents ready for Elasticsearch
        """
        processed_cases = []
        
        for case in cases:
            try:
                # Extract basic case data
                case_data = {
                    'case_id': case['Id'],
                    'case_number': case['CaseNumber'],
                    'subject': case['Subject'],
                    'description': case['Description'],
                    'status': case['Status'],
                    'priority': case['Priority'],
                    'type': case['Type'],
                    'origin': case['Origin'],
                    'reason': case['Reason'],
                    
                    # Account information
                    'account_id': case['AccountId'],
                    'account_name': case['Account']['Name'] if case.get('Account') else None,
                    
                    # Contact information
                    'contact_id': case['ContactId'],
                    'contact_name': case['Contact']['Name'] if case.get('Contact') else None,
                    'contact_email': case['Contact']['Email'] if case.get('Contact') else None,
                    
                    # Supplied information (from web forms, etc.)
                    'supplied_email': case['SuppliedEmail'],
                    'supplied_name': case['SuppliedName'],
                    'supplied_phone': case['SuppliedPhone'],
                    
                    # Dates
                    'created_date': case['CreatedDate'],
                    'closed_date': case['ClosedDate'],
                    'last_modified_date': case['LastModifiedDate'],
                    'sla_start_date': case['SlaStartDate'],
                    'sla_exit_date': case['SlaExitDate'],
                    'escalated_date': case['EscalatedDate'],
                    
                    # Status flags
                    'is_closed': case['IsClosed'],
                    'is_escalated': case['IsEscalated'],
                    
                    # Owner information
                    'owner_id': case['Owner']['Id'] if case.get('Owner') else None,
                    'owner_name': case['Owner']['Name'] if case.get('Owner') else None,
                    'owner_email': case['Owner']['Email'] if case.get('Owner') else None,
                    
                    # Creator information
                    'created_by_id': case['CreatedBy']['Id'] if case.get('CreatedBy') else None,
                    'created_by_name': case['CreatedBy']['Name'] if case.get('CreatedBy') else None,
                    
                    # Parent case (if this is a child case)
                    'parent_case_id': case['ParentId'],
                    'parent_case_number': case['Parent']['CaseNumber'] if case.get('Parent') else None,
                    
                    # Metadata
                    'business_hours_id': case['BusinessHoursId'],
                    'extracted_at': datetime.utcnow().isoformat(),
                    'source': 'salesforce_cases'
                }
                
                # Add case comments if available
                if comments_by_case and case['Id'] in comments_by_case:
                    case_comments = []
                    for comment in comments_by_case[case['Id']]:
                        comment_data = {
                            'comment_id': comment['Id'],
                            'comment_body': comment['CommentBody'],
                            'created_date': comment['CreatedDate'],
                            'created_by_id': comment['CreatedBy']['Id'] if comment.get('CreatedBy') else None,
                            'created_by_name': comment['CreatedBy']['Name'] if comment.get('CreatedBy') else None,
                            'is_published': comment['IsPublished']
                        }
                        case_comments.append(comment_data)
                    
                    case_data['comments'] = case_comments
                    case_data['comment_count'] = len(case_comments)
                else:
                    case_data['comments'] = []
                    case_data['comment_count'] = 0
                
                processed_cases.append(case_data)
                
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
                            "subject": {"type": "keyword"},
                            "description": {"type": "text"},
                            "status": {"type": "keyword"},
                            "priority": {"type": "keyword"},
                            "type": {"type": "keyword"},
                            "origin": {"type": "keyword"},
                            "reason": {"type": "keyword"},
                            
                            "account_id": {"type": "keyword"},
                            "account_name": {"type": "keyword"},
                            
                            "contact_id": {"type": "keyword"},
                            "contact_name": {"type": "keyword"},
                            "contact_email": {"type": "keyword"},
                            
                            "supplied_email": {"type": "keyword"},
                            "supplied_name": {"type": "keyword"},
                            "supplied_phone": {"type": "keyword"},
                            
                            "created_date": {"type": "date"},
                            "closed_date": {"type": "date"},
                            "last_modified_date": {"type": "date"},
                            "sla_start_date": {"type": "date"},
                            "sla_exit_date": {"type": "date"},
                            "escalated_date": {"type": "date"},
                            
                            "is_closed": {"type": "boolean"},
                            "is_escalated": {"type": "boolean"},
                            
                            "owner_id": {"type": "keyword"},
                            "owner_name": {"type": "keyword"},
                            "owner_email": {"type": "keyword"},
                            
                            "created_by_id": {"type": "keyword"},
                            "created_by_name": {"type": "keyword"},
                            
                            "parent_case_id": {"type": "keyword"},
                            "parent_case_number": {"type": "keyword"},
                            
                            "business_hours_id": {"type": "keyword"},
                            "extracted_at": {"type": "date"},
                            "source": {"type": "keyword"},
                            
                            "comments": {
                                "type": "nested",
                                "properties": {
                                    "comment_id": {"type": "keyword"},
                                    "comment_body": {"type": "text"},
                                    "created_date": {"type": "date"},
                                    "created_by_id": {"type": "keyword"},
                                    "created_by_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                    "is_published": {"type": "boolean"}
                                }
                            },
                            "comment_count": {"type": "integer"}
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
    
    def analyze_cases(self, cases: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze cases and provide statistics."""
        
        if not cases:
            return {
                'total_cases': 0,
                'open_cases': 0,
                'closed_cases': 0,
                'by_status': {},
                'by_priority': {},
                'by_type': {},
                'by_origin': {},
                'escalated_cases': 0,
                'with_comments': 0
            }
        
        analysis = {
            'total_cases': len(cases),
            'open_cases': 0,
            'closed_cases': 0,
            'by_status': {},
            'by_priority': {},
            'by_type': {},
            'by_origin': {},
            'escalated_cases': 0,
            'with_comments': 0,
            'total_comments': 0,
            'cases_by_account': {},
            'recent_cases_7d': 0,
            'recent_cases_30d': 0
        }
        
        now = datetime.utcnow()
        seven_days_ago = now - timedelta(days=7)
        thirty_days_ago = now - timedelta(days=30)
        
        for case in cases:
            # Basic counts
            if case['is_closed']:
                analysis['closed_cases'] += 1
            else:
                analysis['open_cases'] += 1
            
            # Status breakdown
            status = case['status']
            analysis['by_status'][status] = analysis['by_status'].get(status, 0) + 1
            
            # Priority breakdown
            priority = case['priority'] or 'No Priority'
            analysis['by_priority'][priority] = analysis['by_priority'].get(priority, 0) + 1
            
            # Type breakdown
            case_type = case['type'] or 'No Type'
            analysis['by_type'][case_type] = analysis['by_type'].get(case_type, 0) + 1
            
            # Origin breakdown
            origin = case['origin'] or 'No Origin'
            analysis['by_origin'][origin] = analysis['by_origin'].get(origin, 0) + 1
            
            # Escalation
            if case['is_escalated']:
                analysis['escalated_cases'] += 1
            
            # Comments
            if case['comment_count'] > 0:
                analysis['with_comments'] += 1
                analysis['total_comments'] += case['comment_count']
            
            # Account grouping
            account_name = case['account_name'] or 'No Account'
            if account_name not in analysis['cases_by_account']:
                analysis['cases_by_account'][account_name] = {
                    'total': 0,
                    'open': 0,
                    'closed': 0,
                    'escalated': 0
                }
            
            analysis['cases_by_account'][account_name]['total'] += 1
            if case['is_closed']:
                analysis['cases_by_account'][account_name]['closed'] += 1
            else:
                analysis['cases_by_account'][account_name]['open'] += 1
            if case['is_escalated']:
                analysis['cases_by_account'][account_name]['escalated'] += 1
            
            # Recent cases
            created_date = datetime.fromisoformat(case['created_date'].replace('Z', '+00:00'))
            if created_date.replace(tzinfo=None) >= seven_days_ago:
                analysis['recent_cases_7d'] += 1
            if created_date.replace(tzinfo=None) >= thirty_days_ago:
                analysis['recent_cases_30d'] += 1
        
        return analysis
    
    def display_analysis(self, analysis: Dict[str, Any]):
        """Display case analysis results."""
        
        print(f"\n🎫 SALESFORCE CASES ANALYSIS")
        print("=" * 40)
        
        print(f"\n📊 Overall Statistics:")
        print(f"   Total Cases: {analysis['total_cases']:,}")
        print(f"   Open Cases: {analysis['open_cases']:,}")
        print(f"   Closed Cases: {analysis['closed_cases']:,}")
        print(f"   Escalated Cases: {analysis['escalated_cases']:,}")
        print(f"   Cases with Comments: {analysis['with_comments']:,}")
        print(f"   Total Comments: {analysis['total_comments']:,}")
        
        print(f"\n📅 Recent Activity:")
        print(f"   Last 7 days: {analysis['recent_cases_7d']:,} cases")
        print(f"   Last 30 days: {analysis['recent_cases_30d']:,} cases")
        
        # Status breakdown
        print(f"\n📋 By Status:")
        for status, count in sorted(analysis['by_status'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / analysis['total_cases']) * 100
            print(f"   {status}: {count:,} ({percentage:.1f}%)")
        
        # Priority breakdown
        print(f"\n⚡ By Priority:")
        for priority, count in sorted(analysis['by_priority'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / analysis['total_cases']) * 100
            print(f"   {priority}: {count:,} ({percentage:.1f}%)")
        
        # Origin breakdown
        print(f"\n📥 By Origin:")
        for origin, count in sorted(analysis['by_origin'].items(), key=lambda x: x[1], reverse=True)[:5]:
            percentage = (count / analysis['total_cases']) * 100
            print(f"   {origin}: {count:,} ({percentage:.1f}%)")
        
        # Top accounts
        print(f"\n🏢 Top Accounts by Case Volume:")
        sorted_accounts = sorted(
            analysis['cases_by_account'].items(),
            key=lambda x: x[1]['total'],
            reverse=True
        )
        
        for account_name, stats in sorted_accounts[:5]:
            print(f"   {account_name}: {stats['total']:,} cases")
            print(f"      Open: {stats['open']}, Closed: {stats['closed']}, Escalated: {stats['escalated']}")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Extract Salesforce Cases and index to Elasticsearch")
    
    parser.add_argument('account_urls', nargs='*', 
                       help='Account URLs to filter cases by')
    parser.add_argument('--account-id', 
                       help='Specific Account ID to filter cases by')
    parser.add_argument('--open-only', action='store_true',
                       help='Only retrieve open cases')
    parser.add_argument('--closed-only', action='store_true',
                       help='Only retrieve closed cases')
    parser.add_argument('--with-comments', action='store_true',
                       help='Include case comments in the output')
    parser.add_argument('--limit', type=int,
                       help='Limit the number of cases retrieved')
    parser.add_argument('--json-only', action='store_true',
                       help='Output JSON only (no Elasticsearch)')
    parser.add_argument('--output-file',
                       help='Output file path for JSON data')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get Elasticsearch config
    es_config = None
    if not args.json_only:
        try:
            es_config = get_elasticsearch_config_from_env()
            if es_config:
                is_valid, error_msg = validate_es_config(es_config)
                if not is_valid:
                    print(f"⚠️  Elasticsearch config invalid: {error_msg}")
                    print("⚠️  Switching to JSON-only mode")
                    args.json_only = True
                    es_config = None
            else:
                es_config = get_elasticsearch_config()
        except Exception as e:
            print(f"⚠️  No Elasticsearch config found, using JSON-only mode")
            args.json_only = True
    
    processor = SalesforceCasesProcessor(es_config)
    
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
    
    # Determine account ID filter
    account_id = args.account_id
    if args.account_urls:
        # Extract account ID from first URL
        account_id = processor.extract_account_id(args.account_urls[0])
        if not account_id:
            print(f"❌ Could not extract account ID from URL: {args.account_urls[0]}")
            sys.exit(1)
        print(f"🎯 Filtering cases for Account: {account_id}")
    
    # Retrieve cases
    print(f"🎫 Retrieving cases from Salesforce...")
    cases = processor.get_cases(
        account_id=account_id,
        open_only=args.open_only,
        closed_only=args.closed_only,
        limit=args.limit
    )
    
    if not cases:
        print("❌ No cases found matching criteria")
        sys.exit(1)
    
    print(f"✅ Retrieved {len(cases)} cases")
    
    # Get case comments if requested
    comments_by_case = None
    if args.with_comments:
        print(f"💬 Retrieving case comments...")
        case_ids = [case['Id'] for case in cases]
        comments_by_case = processor.get_case_comments(case_ids)
    
    # Process cases for Elasticsearch
    processed_cases = processor.process_cases_for_elasticsearch(cases, comments_by_case)
    
    # Analyze cases
    analysis = processor.analyze_cases(processed_cases)
    processor.display_analysis(analysis)
    
    # Index to Elasticsearch if not JSON-only mode
    if not args.json_only and processor.es:
        print(f"\n🔍 Indexing to Elasticsearch...")
        if processor.index_to_elasticsearch(processed_cases):
            print(f"✅ Successfully indexed {len(processed_cases)} cases to Elasticsearch")
            print(f"   Index: {processor.es_config['index']}")
        else:
            print("⚠️  Some cases failed to index to Elasticsearch")
    
    # Save to JSON
    if args.output_file or args.json_only:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = args.output_file or f"salesforce_cases_{timestamp}.json"
        
        output_data = {
            'cases': processed_cases,
            'analysis': analysis,
            'metadata': {
                'extracted_at': datetime.utcnow().isoformat(),
                'total_cases': len(processed_cases),
                'account_id': account_id,
                'filters': {
                    'open_only': args.open_only,
                    'closed_only': args.closed_only,
                    'with_comments': args.with_comments,
                    'limit': args.limit
                }
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\n💾 Data saved to: {filename}")

if __name__ == "__main__":
    main()
