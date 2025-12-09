#!/bin/bash
# Setup script for Salesforce to Elasticsearch Integration Tool Suite
# Updated with account-specific analysis and closed opportunities tools

echo "🚀 Setting up Salesforce to Elasticsearch Integration Tool Suite"
echo "================================================================"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    echo "📥 Install Python 3.8+ from https://python.org or your package manager"
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Check if SF CLI is installed
if ! command -v sf &> /dev/null; then
    echo "❌ Salesforce CLI (sf) is not installed."
    echo "📥 Install it using:"
    echo "   • macOS: brew install sf"
    echo "   • Linux/Windows: Download from https://developer.salesforce.com/tools/sfdxcli"
    echo "⚠️  The tool will still work, but you'll need to authenticate manually."
else
    echo "✅ Salesforce CLI found: $(sf --version)"
fi

# Install Python dependencies
echo ""
echo "📦 Installing Python dependencies..."
if pip install -r requirements.txt --break-system-packages; then
    echo "✅ Dependencies installed successfully"
else
    echo "⚠️  Some dependencies may have failed to install"
    echo "Try manually: pip3 install simple-salesforce elasticsearch requests --break-system-packages"
fi

# Make scripts executable
echo ""
echo "🔧 Making scripts executable..."
chmod +x *.py
echo "✅ Scripts are now executable"

# Create examples directory if it doesn't exist
if [ ! -d "examples" ]; then
    mkdir -p examples
    echo "📁 Created examples directory"
fi

# Test basic imports
echo ""
echo "🧪 Testing basic imports..."
if python3 -c "import simple_salesforce; import elasticsearch; print('✅ Core dependencies working')"; then
    echo "✅ Core dependencies are working"
else
    echo "❌ Some core dependencies are missing"
    echo "Please run: pip install simple-salesforce elasticsearch requests --break-system-packages"
fi

# Display available tools
echo ""
echo "🎯 AVAILABLE TOOLS"
echo "=================="

echo ""
echo "📊 CORE OPPORTUNITY PROCESSING:"
echo "   • interactive_sf_to_es.py        - Interactive menu-driven interface"
echo "   • sf_to_elasticsearch.py        - Process single opportunity URL → Elasticsearch"
echo "   • batch_sf_to_elasticsearch.py  - Batch process multiple URLs → Elasticsearch"

echo ""
echo "📋 JSON EXPLORATION & TESTING:"
echo "   • sf_to_json.py                 - Single opportunity → JSON (no ES needed)"
echo "   • sf_explore_json.py            - Discover all available fields in your org"

echo ""
echo "🎯 CLOSED OPPORTUNITIES ANALYSIS:"
echo "   • sf_closed_simple.py           - Quick closed opportunities analysis"
echo "   • sf_closed_opportunities.py    - Full closed opportunities analysis + ES"
echo "   • sf_sales_dashboard.py         - Real-time sales dashboard"

echo ""
echo "🏢 ACCOUNT-SPECIFIC ANALYSIS:"
echo "   • sf_account_simple.py          - Quick account opportunity analysis"
echo "   • sf_account_opportunities.py   - Full account analysis + ES integration"

echo ""
echo "🔧 DEBUG & TROUBLESHOOTING:"
echo "   • debug_batch_sf_to_es.py       - Debug version of batch processor"
echo "   • verify_soql.py                - Verify SOQL queries without executing"
echo "   • test_validation.py            - Comprehensive validation tests"
echo "   • test_imports.py               - Import verification tests"

echo ""
echo "📖 DOCUMENTATION:"
echo "   • README.md                     - Complete usage guide"
echo "   • CLOSED_OPPORTUNITIES_GUIDE.md - Closed opportunities analysis guide"
echo "   • ACCOUNT_OPPORTUNITIES_GUIDE.md - Account-specific analysis guide"  
echo "   • ELASTICSEARCH_ACCOUNT_CONFIG.md - ES configuration for account tools"
echo "   • TCV_FIELD_FIX.md             - Field troubleshooting guide"
echo "   • FILE_LISTING.md              - Complete file overview"

# Test connections (optional)
echo ""
echo "🔍 Would you like to test the connections now? (y/N)"
read -r test_connections

if [[ $test_connections =~ ^[Yy]$ ]]; then
    echo ""
    echo "🧪 Testing connections..."
    echo "Choose a test:"
    echo "1. Interactive tool (full ES configuration)"
    echo "2. Simple JSON test (no ES needed)"
    echo "3. Import validation only"
    read -p "Enter choice (1/2/3): " test_choice
    
    case $test_choice in
        1)
            echo "🔄 Starting interactive tool..."
            python3 interactive_sf_to_es.py
            ;;
        2)
            echo "📋 Testing JSON output (you'll need an opportunity URL)..."
            echo "Example: python3 sf_to_json.py 'your_opportunity_url'"
            ;;
        3)
            echo "🧪 Running import validation..."
            python3 test_validation.py
            ;;
        *)
            echo "ℹ️  Skipping connection test"
            ;;
    esac
else
    echo ""
    echo "🎉 Setup complete!"
    echo ""
    echo "📖 QUICK START GUIDE"
    echo "===================="
    
    echo ""
    echo "🔐 1. Authenticate with Salesforce:"
    echo "   sf org login web -r https://elastic.my.salesforce.com"
    
    echo ""
    echo "🧪 2. Test with JSON output first (no ES needed):"
    echo "   python3 sf_to_json.py 'your_opportunity_url'"
    echo "   python3 sf_closed_simple.py"
    echo "   python3 sf_account_simple.py 'your_account_url'"
    
    echo ""
    echo "⚙️ 3. Configure Elasticsearch (for production):"
    echo "   ./configure_env.sh                    # Interactive configuration"
    echo "   # OR set environment variables:"
    echo "   export ES_CLUSTER_URL='your_cluster_url'"
    echo "   export ES_USERNAME='your_username'"
    echo "   export ES_PASSWORD='your_password'"
    echo "   export ES_INDEX='opportunity-data'"
    
    echo ""
    echo "🚀 4. Start with these tools:"
    echo "   python3 interactive_sf_to_es.py      # Full interactive interface"
    echo "   python3 sf_closed_simple.py          # Quick closed opps analysis"
    echo "   python3 sf_account_simple.py 'url'   # Account-specific analysis"
    
    echo ""
    echo "📊 COMMON USE CASES:"
    echo ""
    echo "🎯 Opportunity Analysis:"
    echo "   python3 sf_to_elasticsearch.py 'opportunity_url'"
    echo "   python3 batch_sf_to_elasticsearch.py urls.txt"
    
    echo ""
    echo "📈 Sales Performance:"
    echo "   python3 sf_closed_simple.py --won-only"
    echo "   python3 sf_sales_dashboard.py --one-time"
    
    echo ""
    echo "🏢 Account Analysis:"
    echo "   python3 sf_account_simple.py 'account_url'"
    echo "   python3 sf_account_opportunities.py --accounts-file key_accounts.txt"
    
    echo ""
    echo "🔍 Field Discovery:"
    echo "   python3 sf_explore_json.py 'opportunity_url'"
    
    echo ""
    echo "🧪 VALIDATION & TESTING:"
    echo "   python3 test_validation.py           # Comprehensive tests"
    echo "   python3 verify_soql.py 'url'         # Query verification"
    echo "   python3 debug_batch_sf_to_es.py file.txt  # Debug batch issues"
    
    echo ""
    echo "📚 DOCUMENTATION:"
    echo "   • README.md - Complete documentation"
    echo "   • *_GUIDE.md files - Specific tool guides"
    echo "   • examples/ - Example files and configurations"
    
    echo ""
    echo "💡 TIP: Start with JSON tools for testing, then move to Elasticsearch for production!"
fi

echo ""
echo "✨ Happy analyzing! Your fraud detection expertise will be perfect for sales data patterns! 🎯"
