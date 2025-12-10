#!/usr/bin/env python3
"""
Opportunity Cases Analysis

Find and analyze cases related to specific opportunities.
This looks for cases linked to the same account as the opportunities.

Usage:
    python3 sf_opportunity_cases.py <opportunity_url>
    python3 sf_opportunity_cases.py <opportunity_url1> <opportunity_url2>
    python3 sf_opportunity_cases.py --file opportunities.txt

Examples:
    python3 sf_opportunity_cases.py "https://elastic.lightning.force.com/lightning/r/Opportunity/006Vv00000IZaFxIAL/view"
    python3 sf_opportunity_cases.py --file opportunity_urls.txt --priority High
"""

import sys
import json
import re
import os
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sf_auth import get_salesforce_connection

def extract_opportunity_id(url: str) -> Optional[str]:
    """Extract Salesforce Opportunity ID from URL."""
    patterns = [
        r'/([A-Za-z0-9]{15,18})',
        r'/Opportunity/([A-Za-z0-9]{15,18})',
        r'006[A-Za-z0-9]{12,15}',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            opp_id = match.group(1) if len(match.groups()) > 0 else match.group(0)
            if opp_id.startswith('006') and len(opp_id) >= 15:
                return opp_id
    
    # Try as raw ID
    if url.startswith('006') and 15 <= len(url) <= 18:
        return url
    
    return None

def get_opportunities_info(sf, opportunity_ids: List[str]) -> Dict[str, Any]:
    """Get opportunity information including account details."""
    
    if not opportunity_ids:
        return {}
    
    opp_ids_str = "','".join(opportunity_ids)
    
    try:
        query = f"""
        SELECT 
            Id, Name, AccountId, Account.Name, Amount, StageName, 
            CloseDate, IsWon, IsClosed, Owner.Name, CreatedDate
        FROM Opportunity 
        WHERE Id IN ('{opp_ids_str}')
        ORDER BY Name
        """
        
        result = sf.query(query)
        
        opportunities_info = {}
        account_ids = set()
        
        for record in result['records']:
            opp_info = {
                'id': record['Id'],
                'name': record['Name'],
                'account_id': record['AccountId'],
                'account_name': record['Account']['Name'] if record.get('Account') else None,
                'amount': record.get('Amount'),
                'stage': record['StageName'],
                'close_date': record.get('CloseDate'),
                'is_won': record.get('IsWon', False),
                'is_closed': record.get('IsClosed', False),
                'owner': record['Owner']['Name'] if record.get('Owner') else None,
                'created_date': record['CreatedDate']
            }
            opportunities_info[record['Id']] = opp_info
            
            if record['AccountId']:
                account_ids.add(record['AccountId'])
        
        return {
            'opportunities': opportunities_info,
            'account_ids': list(account_ids)
        }
        
    except Exception as e:
        print(f"❌ Error retrieving opportunity info: {str(e)}")
        return {'opportunities': {}, 'account_ids': []}

def get_cases_for_accounts(sf, account_ids: List[str], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get cases for the specified accounts (related to opportunities)."""
    
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
            Reason
        FROM Case 
        WHERE {where_clause}
        ORDER BY Account.Name, CreatedDate DESC
        """
        
        if filters.get('limit'):
            query += f" LIMIT {filters['limit']}"
        
        print(f"🔍 Querying cases for opportunity-related accounts...")
        result = sf.query_all(query)
        
        print(f"📋 Found {result['totalSize']} cases")
        return result['records']
        
    except Exception as e:
        print(f"❌ Error querying cases: {str(e)}")
        return []

def get_case_comments(sf, case_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Get comments for the specified cases."""
    
    if not case_ids:
        return {}
    
    # Limit to prevent too large queries
    if len(case_ids) > 100:
        print(f"⚠️  Limiting case comments query to first 100 cases")
        case_ids = case_ids[:100]
    
    case_ids_str = "','".join(case_ids)
    
    try:
        query = f"""
        SELECT 
            Id, ParentId, CommentBody, IsPublished, 
            CreatedDate, CreatedBy.Name, CreatedBy.Email
        FROM CaseComment 
        WHERE ParentId IN ('{case_ids_str}')
        ORDER BY ParentId, CreatedDate ASC
        """
        
        result = sf.query_all(query)
        
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
                'created_by_email': comment['CreatedBy']['Email'] if comment.get('CreatedBy') else None
            })
        
        print(f"💬 Retrieved comments for {len(comments_by_case)} cases")
        return dict(comments_by_case)
        
    except Exception as e:
        print(f"⚠️  Error retrieving case comments: {str(e)}")
        return {}

def analyze_opportunity_cases(opportunities_info: Dict[str, Any], cases: List[Dict[str, Any]], 
                            case_comments: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Analyze cases related to opportunities."""
    
    if not cases:
        return {
            'total_cases': 0,
            'total_opportunities': len(opportunities_info.get('opportunities', {})),
            'by_opportunity': {},
            'overall_stats': {
                'total_cases': 0,
                'open_cases': 0,
                'closed_cases': 0
            }
        }
    
    opportunities = opportunities_info.get('opportunities', {})
    
    # Overall stats
    total_cases = len(cases)
    open_cases = sum(1 for case in cases if not case['IsClosed'])
    closed_cases = total_cases - open_cases
    total_comments = sum(len(comments) for comments in case_comments.values())
    
    # Group cases by account, then link to opportunities
    cases_by_account = defaultdict(list)
    for case in cases:
        account_id = case['AccountId']
        cases_by_account[account_id].append(case)
    
    # Create analysis by opportunity
    by_opportunity = {}
    
    for opp_id, opp_info in opportunities.items():
        account_id = opp_info['account_id']
        account_cases = cases_by_account.get(account_id, [])
        
        # Calculate case stats for this opportunity's account
        opp_open_cases = sum(1 for case in account_cases if not case['IsClosed'])
        opp_closed_cases = len(account_cases) - opp_open_cases
        opp_comments = sum(len(case_comments.get(case['Id'], [])) for case in account_cases)
        
        # Get case priorities and statuses
        priorities = Counter(case.get('Priority', 'None') for case in account_cases)
        statuses = Counter(case.get('Status', 'None') for case in account_cases)
        types = Counter(case.get('Type', 'None') for case in account_cases)
        
        # Calculate case age stats
        now = datetime.utcnow()
        case_ages = []
        for case in account_cases:
            try:
                created = datetime.fromisoformat(case['CreatedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                if case['IsClosed'] and case['ClosedDate']:
                    closed = datetime.fromisoformat(case['ClosedDate'].replace('Z', '+00:00').replace('+00:00', ''))
                    age_days = (closed - created).days
                else:
                    age_days = (now - created).days
                case_ages.append(age_days)
            except:
                continue
        
        avg_case_age = sum(case_ages) / len(case_ages) if case_ages else 0
        
        by_opportunity[opp_id] = {
            'opportunity_info': opp_info,
            'cases': account_cases,
            'stats': {
                'total_cases': len(account_cases),
                'open_cases': opp_open_cases,
                'closed_cases': opp_closed_cases,
                'close_rate': (opp_closed_cases / len(account_cases) * 100) if account_cases else 0,
                'total_comments': opp_comments,
                'avg_case_age_days': avg_case_age,
                'priorities': dict(priorities),
                'statuses': dict(statuses),
                'types': dict(types)
            }
        }
    
    # Overall breakdowns
    all_priorities = Counter(case.get('Priority', 'None') for case in cases)
    all_statuses = Counter(case.get('Status', 'None') for case in cases)
    all_types = Counter(case.get('Type', 'None') for case in cases)
    
    return {
        'total_cases': total_cases,
        'total_opportunities': len(opportunities),
        'by_opportunity': by_opportunity,
        'overall_stats': {
            'total_cases': total_cases,
            'open_cases': open_cases,
            'closed_cases': closed_cases,
            'close_rate': (closed_cases / total_cases * 100) if total_cases > 0 else 0,
            'total_comments': total_comments,
            'avg_comments_per_case': total_comments / total_cases if total_cases > 0 else 0,
            'priority_breakdown': dict(all_priorities),
            'status_breakdown': dict(all_statuses),
            'type_breakdown': dict(all_types)
        }
    }

def display_analysis(analysis: Dict[str, Any]):
    """Display the opportunity-cases analysis."""
    
    print(f"\n🎯 OPPORTUNITY-RELATED CASES ANALYSIS")
    print("=" * 45)
    
    stats = analysis['overall_stats']
    print(f"\n📊 Overall Statistics:")
    print(f"   Opportunities Analyzed: {analysis['total_opportunities']}")
    print(f"   Related Cases: {stats['total_cases']:,}")
    print(f"   Open Cases: {stats['open_cases']:,}")
    print(f"   Closed Cases: {stats['closed_cases']:,}")
    print(f"   Case Close Rate: {stats['close_rate']:.1f}%")
    print(f"   Total Comments: {stats['total_comments']:,}")
    print(f"   Avg Comments/Case: {stats['avg_comments_per_case']:.1f}")
    
    # Priority breakdown
    if stats.get('priority_breakdown'):
        print(f"\n📈 Case Priority Breakdown:")
        for priority, count in sorted(stats['priority_breakdown'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / stats['total_cases'] * 100) if stats['total_cases'] > 0 else 0
            print(f"   {priority}: {count:,} ({percentage:.1f}%)")
    
    if not analysis['by_opportunity']:
        print(f"\n📋 No opportunities with related cases found.")
        return
    
    # Sort opportunities by case count
    sorted_opportunities = sorted(
        analysis['by_opportunity'].items(),
        key=lambda x: x[1]['stats']['total_cases'],
        reverse=True
    )
    
    print(f"\n💼 BREAKDOWN BY OPPORTUNITY:")
    print("=" * 40)
    
    for i, (opp_id, data) in enumerate(sorted_opportunities, 1):
        opp_info = data['opportunity_info']
        opp_stats = data['stats']
        
        print(f"\n{i}. {opp_info['name']}")
        print(f"    Opportunity ID: {opp_id}")
        print(f"    Account: {opp_info['account_name']}")
        print(f"    Stage: {opp_info['stage']}")
        
        if opp_info['amount']:
            print(f"    Amount: ${opp_info['amount']:,.2f}")
        
        if opp_info['close_date']:
            print(f"    Close Date: {opp_info['close_date']}")
        
        status = "WON" if opp_info['is_won'] else "LOST" if opp_info['is_closed'] else "OPEN"
        print(f"    Status: {status}")
        
        print(f"    Related Cases: {opp_stats['total_cases']} (Open: {opp_stats['open_cases']}, Closed: {opp_stats['closed_cases']})")
        
        if opp_stats['total_cases'] > 0:
            print(f"    Case Close Rate: {opp_stats['close_rate']:.1f}%")
            print(f"    Comments: {opp_stats['total_comments']}")
            print(f"    Avg Case Age: {opp_stats['avg_case_age_days']:.1f} days")
            
            # Show top case priorities
            if opp_stats['priorities']:
                top_priorities = sorted(opp_stats['priorities'].items(), key=lambda x: x[1], reverse=True)
                print(f"    Case Priorities: {', '.join(f'{p}({c})' for p, c in top_priorities[:3])}")
            
            # Show recent cases
            recent_cases = sorted(data['cases'], key=lambda x: x['CreatedDate'], reverse=True)[:3]
            if recent_cases:
                print(f"    Recent Cases:")
                for j, case in enumerate(recent_cases, 1):
                    status = case['Status'] or 'No Status'
                    priority = case['Priority'] or 'No Priority'
                    created = case['CreatedDate'][:10] if case['CreatedDate'] else 'Unknown'
                    subject = case['Subject'][:35] + "..." if case['Subject'] and len(case['Subject']) > 35 else case['Subject']
                    print(f"      {j}. {case['CaseNumber']} - {subject}")
                    print(f"         {status} | {priority} | {created}")

def save_to_json(analysis: Dict[str, Any], cases: List[Dict[str, Any]], 
                case_comments: Dict[str, List[Dict[str, Any]]], 
                opportunities_info: Dict[str, Any], filename: str):
    """Save analysis and data to JSON file."""
    
    output_data = {
        'analysis': analysis,
        'raw_data': {
            'opportunities': opportunities_info,
            'cases': cases,
            'case_comments': case_comments
        },
        'metadata': {
            'generated_at': datetime.utcnow().isoformat(),
            'total_opportunities': analysis['total_opportunities'],
            'total_cases': len(cases),
            'total_comments': sum(len(comments) for comments in case_comments.values())
        }
    }
    
    with open(filename, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)
    
    print(f"\n💾 Data saved to: {filename}")

def main():
    """Main function."""
    
    parser = argparse.ArgumentParser(description='Analyze cases related to specific opportunities')
    
    # Opportunity specification
    parser.add_argument('opportunity_urls', nargs='*', help='Opportunity URLs to analyze')
    parser.add_argument('--file', dest='opportunities_file', help='File containing opportunity URLs (one per line)')
    
    # Filters
    parser.add_argument('--open-only', action='store_true', help='Only open cases')
    parser.add_argument('--closed-only', action='store_true', help='Only closed cases')
    parser.add_argument('--priority', choices=['High', 'Medium', 'Low'], help='Filter by case priority')
    parser.add_argument('--status', help='Filter by case status')
    parser.add_argument('--type', help='Filter by case type')
    parser.add_argument('--date-from', help='Filter cases created from date (YYYY-MM-DD)')
    parser.add_argument('--date-to', help='Filter cases created to date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, help='Limit number of cases returned')
    
    # Output
    parser.add_argument('--output', help='Output JSON filename')
    parser.add_argument('--no-comments', action='store_true', help='Skip case comments retrieval')
    
    args = parser.parse_args()
    
    # Get opportunity URLs/IDs
    opportunity_urls = args.opportunity_urls or []
    if args.opportunities_file:
        if not os.path.exists(args.opportunities_file):
            print(f"Error: File '{args.opportunities_file}' does not exist.")
            sys.exit(1)
        
        with open(args.opportunities_file, 'r') as f:
            file_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            opportunity_urls.extend(file_urls)
    
    if not opportunity_urls:
        parser.print_help()
        print(f"\nError: Must provide opportunity URLs or --file parameter")
        sys.exit(1)
    
    # Connect to Salesforce
    try:
        sf = get_salesforce_connection()
        print(f"✅ Connected to Salesforce")
    except Exception as e:
        print(f"❌ Failed to connect to Salesforce: {str(e)}")
        sys.exit(1)
    
    # Extract opportunity IDs
    opportunity_ids = []
    for url in opportunity_urls:
        opp_id = extract_opportunity_id(url)
        if opp_id:
            opportunity_ids.append(opp_id)
        else:
            print(f"⚠️  Invalid opportunity URL: {url}")
    
    if not opportunity_ids:
        print(f"❌ No valid opportunity IDs found")
        sys.exit(1)
    
    print(f"🔍 Analyzing cases related to {len(opportunity_ids)} opportunity(ies)")
    
    # Get opportunity information and account IDs
    opportunities_info = get_opportunities_info(sf, opportunity_ids)
    account_ids = opportunities_info.get('account_ids', [])
    
    if not account_ids:
        print(f"❌ No accounts found for the specified opportunities")
        sys.exit(1)
    
    print(f"✅ Found {len(account_ids)} related accounts")
    
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
    
    # Get cases for the related accounts
    cases = get_cases_for_accounts(sf, account_ids, filters)
    
    if not cases:
        print(f"📋 No cases found for the opportunity-related accounts")
        print(f"💡 These opportunities may not have any associated customer service cases")
        return
    
    # Get case comments
    case_comments = {}
    if not args.no_comments and cases:
        case_ids = [case['Id'] for case in cases]
        case_comments = get_case_comments(sf, case_ids)
    
    # Analyze data
    analysis = analyze_opportunity_cases(opportunities_info, cases, case_comments)
    
    # Display results
    display_analysis(analysis)
    
    # Save to JSON
    if args.output or len(cases) > 0:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = args.output or f"opportunity_cases_{timestamp}.json"
        save_to_json(analysis, cases, case_comments, opportunities_info, filename)

if __name__ == "__main__":
    main()
