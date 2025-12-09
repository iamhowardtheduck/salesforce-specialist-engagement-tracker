#!/usr/bin/env python3
"""
Simple Account Opportunities Analysis

Quick script to get closed opportunities for specific accounts.
Perfect for analyzing key accounts or customer performance.

Usage:
    python sf_account_simple.py <account_url1> [account_url2] [...]
    python sf_account_simple.py --file accounts.txt
    
Examples:
    # Single account
    python sf_account_simple.py "https://elastic.lightning.force.com/lightning/r/Account/001b000000kFpsaAAC/view"
    
    # Multiple accounts
    python sf_account_simple.py "account_url1" "account_url2"
    
    # From file
    python sf_account_simple.py --file key_accounts.txt
    
    # Only won opportunities
    python sf_account_simple.py --won-only "account_url"
"""

import sys
import json
import logging
import os
import argparse
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sf_auth import get_salesforce_connection

# Simple logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def extract_account_id(url: str) -> Optional[str]:
    """Extract account ID from Salesforce URL."""
    # Pattern for Salesforce account ID (starts with 001)
    patterns = [
        r'/([A-Za-z0-9]{15,18})',  # Generic ID pattern
        r'/Account/([A-Za-z0-9]{15,18})',  # Explicit account pattern
        r'001[A-Za-z0-9]{12,15}',  # Account-specific pattern
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            account_id = match.group(1) if len(match.groups()) > 0 else match.group(0)
            if account_id.startswith('001') and len(account_id) >= 15:
                return account_id
    
    # If it looks like it might be a raw Account ID
    if url.startswith('001') and 15 <= len(url) <= 18:
        return url
    
    return None

def get_accounts_from_file(file_path: str) -> List[str]:
    """Extract account IDs from file."""
    account_ids = []
    
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        for line in lines:
            account_id = extract_account_id(line)
            if account_id:
                account_ids.append(account_id)
            else:
                print(f"⚠️  Could not extract account ID from: {line}")
        
        return account_ids
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return []

def query_account_opportunities(sf, account_ids: List[str], won_only=False, lost_only=False):
    """Query opportunities for specific accounts."""
    
    if not account_ids:
        return [], {}
    
    # Get account information first
    accounts_query = f"""
    SELECT Id, Name, Type, Industry, AnnualRevenue, 
           NumberOfEmployees, BillingCity, BillingState
    FROM Account 
    WHERE Id IN ('{"','".join(account_ids)}')
    """
    
    # Get opportunities
    opps_query = f"""
    SELECT Id, Name, Account.Id, Account.Name, CloseDate, Amount, 
           StageName, IsWon, Type, CreatedDate, Owner.Name
    FROM Opportunity 
    WHERE IsClosed = true AND AccountId IN ('{"','".join(account_ids)}')
    """
    
    if won_only:
        opps_query += " AND IsWon = true"
    elif lost_only:
        opps_query += " AND IsWon = false"
    
    opps_query += " ORDER BY Account.Name, CloseDate DESC, Amount DESC"
    
    try:
        print("📡 Querying account information...")
        accounts_result = sf.query(accounts_query)
        
        print("📡 Querying opportunities...")
        opps_result = sf.query_all(opps_query)
        
        # Build account info dict
        account_info = {}
        for acc in accounts_result['records']:
            account_info[acc['Id']] = acc
        
        opportunities = opps_result['records']
        
        print(f"✅ Found {len(opportunities)} opportunities across {len(account_info)} accounts")
        
        return opportunities, account_info
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return [], {}

def analyze_opportunities(opportunities, account_info):
    """Analyze opportunities by account."""
    
    if not opportunities:
        return {}
    
    # Group by account
    by_account = {}
    
    for opp in opportunities:
        account_id = opp['Account']['Id']
        account_name = opp['Account']['Name']
        
        if account_id not in by_account:
            by_account[account_id] = {
                'account_name': account_name,
                'account_info': account_info.get(account_id, {}),
                'opportunities': [],
                'total_count': 0,
                'won_count': 0,
                'lost_count': 0,
                'total_amount': 0,
                'won_amount': 0,
                'lost_amount': 0
            }
        
        by_account[account_id]['opportunities'].append(opp)
        by_account[account_id]['total_count'] += 1
        
        amount = opp['Amount'] or 0
        by_account[account_id]['total_amount'] += amount
        
        if opp['IsWon']:
            by_account[account_id]['won_count'] += 1
            by_account[account_id]['won_amount'] += amount
        else:
            by_account[account_id]['lost_count'] += 1
            by_account[account_id]['lost_amount'] += amount
    
    # Calculate derived metrics
    for account_data in by_account.values():
        if account_data['total_count'] > 0:
            account_data['win_rate'] = (account_data['won_count'] / account_data['total_count']) * 100
            account_data['avg_deal_size'] = account_data['total_amount'] / account_data['total_count']
        else:
            account_data['win_rate'] = 0
            account_data['avg_deal_size'] = 0
    
    return by_account

def print_analysis(by_account):
    """Print formatted analysis."""
    
    if not by_account:
        print("❌ No data to analyze")
        return
    
    # Overall stats
    total_accounts = len(by_account)
    total_opps = sum(data['total_count'] for data in by_account.values())
    total_won = sum(data['won_count'] for data in by_account.values())
    total_amount = sum(data['total_amount'] for data in by_account.values())
    total_won_amount = sum(data['won_amount'] for data in by_account.values())
    
    overall_win_rate = (total_won / total_opps * 100) if total_opps > 0 else 0
    
    print(f"\n🎯 ACCOUNT OPPORTUNITIES SUMMARY")
    print("=" * 50)
    print(f"📊 Overall: {total_accounts} accounts, {total_opps} opportunities")
    print(f"💰 Total Revenue: ${total_amount:,.2f}")
    print(f"🏆 Won: {total_won} deals (${total_won_amount:,.2f}) - {overall_win_rate:.1f}% win rate")
    
    # Sort accounts by revenue
    sorted_accounts = sorted(
        by_account.items(), 
        key=lambda x: x[1]['total_amount'], 
        reverse=True
    )
    
    print(f"\n📋 BY ACCOUNT:")
    print("=" * 50)
    
    for i, (account_id, data) in enumerate(sorted_accounts, 1):
        account_info = data['account_info']
        
        print(f"\n{i}. {data['account_name']}")
        
        # Account details
        if account_info:
            details = []
            if account_info.get('Type'):
                details.append(f"Type: {account_info['Type']}")
            if account_info.get('Industry'):
                details.append(f"Industry: {account_info['Industry']}")
            if account_info.get('AnnualRevenue'):
                details.append(f"Revenue: ${account_info['AnnualRevenue']:,.0f}")
            if account_info.get('NumberOfEmployees'):
                details.append(f"Employees: {account_info['NumberOfEmployees']:,}")
            
            location = []
            if account_info.get('BillingCity'):
                location.append(account_info['BillingCity'])
            if account_info.get('BillingState'):
                location.append(account_info['BillingState'])
            if location:
                details.append(f"Location: {', '.join(location)}")
            
            if details:
                print(f"   {' | '.join(details)}")
        
        # Opportunity stats
        print(f"   📈 {data['total_count']} opportunities: {data['won_count']} won, {data['lost_count']} lost ({data['win_rate']:.1f}% win rate)")
        print(f"   💰 ${data['total_amount']:,.2f} total (${data['won_amount']:,.2f} won, ${data['lost_amount']:,.2f} lost)")
        print(f"   📊 Average deal: ${data['avg_deal_size']:,.2f}")
        
        # Top 3 deals
        top_deals = sorted(data['opportunities'], key=lambda x: x['Amount'] or 0, reverse=True)[:3]
        if top_deals:
            print(f"   🏆 Top deals:")
            for j, deal in enumerate(top_deals, 1):
                status = "WON" if deal['IsWon'] else "LOST"
                amount = deal['Amount'] or 0
                close_date = deal['CloseDate']
                print(f"      {j}. ${amount:,.2f} - {deal['Name']} ({close_date}) [{status}]")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Simple account opportunities analysis')
    parser.add_argument('account_urls', nargs='*', help='Account URLs or IDs')
    parser.add_argument('--file', dest='accounts_file', help='File with account URLs (one per line)')
    parser.add_argument('--won-only', action='store_true', help='Only won opportunities')
    parser.add_argument('--lost-only', action='store_true', help='Only lost opportunities')
    parser.add_argument('--save-json', action='store_true', help='Save results to JSON')
    
    args = parser.parse_args()
    
    if args.won_only and args.lost_only:
        print("Error: Cannot specify both --won-only and --lost-only")
        sys.exit(1)
    
    # Get account URLs/IDs
    account_urls = args.account_urls or []
    
    if args.accounts_file:
        if not os.path.exists(args.accounts_file):
            print(f"Error: File '{args.accounts_file}' not found")
            sys.exit(1)
        file_accounts = get_accounts_from_file(args.accounts_file)
        account_urls.extend([acc for acc in file_accounts if acc])  # Add file IDs as if they were URLs
    
    if not account_urls:
        parser.print_help()
        print(f"\nError: Must provide account URLs or --file")
        sys.exit(1)
    
    print("🏢 Simple Account Opportunities Analysis")
    print("=" * 45)
    
    # Extract account IDs
    account_ids = []
    print(f"\n🔍 Processing {len(account_urls)} account reference(s)...")
    
    for ref in account_urls:
        account_id = extract_account_id(ref)
        if account_id:
            account_ids.append(account_id)
            print(f"  ✅ {account_id}")
        else:
            print(f"  ❌ Invalid: {ref}")
    
    if not account_ids:
        print("❌ No valid account IDs found")
        sys.exit(1)
    
    # Remove duplicates
    account_ids = list(set(account_ids))
    
    # Connect to Salesforce
    try:
        sf = get_salesforce_connection()
        print("✅ Connected to Salesforce")
    except Exception as e:
        print(f"❌ Salesforce connection failed: {e}")
        sys.exit(1)
    
    # Query opportunities
    opportunities, account_info = query_account_opportunities(
        sf, account_ids, args.won_only, args.lost_only
    )
    
    if not opportunities:
        print("❌ No opportunities found")
        sys.exit(1)
    
    # Analyze and display
    analysis = analyze_opportunities(opportunities, account_info)
    print_analysis(analysis)
    
    # Save JSON if requested
    if args.save_json:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"account_opportunities_{timestamp}.json"
        
        output = {
            'analysis_summary': {
                'total_accounts': len(analysis),
                'total_opportunities': len(opportunities),
                'account_ids': account_ids
            },
            'by_account': analysis,
            'opportunities': opportunities,
            'account_info': account_info,
            'generated_at': datetime.utcnow().isoformat()
        }
        
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        print(f"\n💾 Data saved to: {filename}")

if __name__ == "__main__":
    main()
