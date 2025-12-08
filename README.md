# salesforce-specialist-engagement-tracker
A tool to gather and visualize specialist engagements.

# Salesforce to Elasticsearch Integration Tool

This tool extracts opportunity data from Salesforce and indexes it into Elasticsearch. It provides multiple interfaces for different use cases: single URL processing, batch processing, and interactive mode.

## Features

- 🔐 **Secure Authentication**: Uses Salesforce CLI for OAuth authentication
- 📊 **Data Extraction**: Retrieves opportunity details including name, account, amounts, and dates
- 🔄 **Batch Processing**: Handle multiple opportunities efficiently
- 🔍 **Interactive Mode**: User-friendly interface for exploration
- 📝 **Comprehensive Logging**: Detailed logs for monitoring and debugging
- ⚡ **Bulk Operations**: Optimized for large datasets

## Prerequisites

1. **Salesforce CLI**: Install using `brew install sf` (macOS) or equivalent
2. **Python 3.7+**: Required for all scripts
3. **Elasticsearch Access**: Valid credentials for your cluster

## Installation

1. Install required Python packages:
```bash
pip install simple-salesforce elasticsearch requests --break-system-packages
```

2. Ensure you have access to:
   - Salesforce instance: https://elastic.my.salesforce.com
   - Elasticsearch cluster: https://er-tracker.es.us-east-2.aws.elastic-cloud.com

## Configuration

The tool uses interactive configuration prompts to gather your Elasticsearch settings. When you run any script, you'll be prompted for:

### Elasticsearch Settings:
- **Cluster URL**: Your Elasticsearch cluster endpoint
- **Index Name**: Target index (defaults to "specialist-engagements")
- **Authentication**: Choose between:
  - Username and password
  - API key

### Authentication Options:

**Option 1: Username/Password**
```
Username: your_username
Password: your_password
```

**Option 2: API Key**
```
API key: your_base64_encoded_api_key
```

### Environment Variables (for automation):
For non-interactive use, set these environment variables:

**Username/Password:**
```bash
export ES_CLUSTER_URL="https://your-cluster.es.region.aws.elastic-cloud.com"
export ES_USERNAME="your_username"
export ES_PASSWORD="your_password"
export ES_INDEX="specialist-engagements"  # optional
```

**API Key:**
```bash
export ES_CLUSTER_URL="https://your-cluster.es.region.aws.elastic-cloud.com"
export ES_API_KEY="your_base64_encoded_api_key"
export ES_INDEX="specialist-engagements"  # optional
```

### Security Features:
- SSL certificate verification is disabled for flexibility
- Credentials are never stored in files
- API key support for enhanced security

## Usage

### 1. Interactive Mode (Recommended for beginners)

```bash
python interactive_sf_to_es.py
```

This provides a menu-driven interface that guides you through:
- Testing connections
- Processing single URLs
- Batch processing files
- Viewing configuration
- Checking index status

### 2. Single Opportunity Processing

```bash
python sf_to_elasticsearch.py "https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view"
```

### 3. Batch Processing

```bash
python batch_sf_to_elasticsearch.py opportunity_urls.txt
```

Create a text file with one opportunity URL per line:
```
https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000XXXXXX/view
https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000YYYYYY/view
https://elastic.lightning.force.com/lightning/r/Opportunity/0064R00000ZZZZZZ/view
```

## Data Fields

The tool extracts and indexes the following fields:

| Salesforce Field | Elasticsearch Field | Description |
|------------------|---------------------|-------------|
| Id | opportunity_id | Unique opportunity identifier |
| Name | opportunity_name | Opportunity name |
| Account.Name | account_name | Associated account name |
| CloseDate | close_date | Expected close date |
| Amount | amount | Opportunity amount |
| TCV__c | tcv_amount | Total Contract Value |
| - | extracted_at | Timestamp of extraction |
| - | source | Source system identifier |

## URL Formats Supported

The tool can extract opportunity IDs from these URL formats:

- Lightning Experience: `https://elastic.lightning.force.com/lightning/r/Opportunity/006XXXXXXXXXXXXX/view`
- Classic: `https://elastic.my.salesforce.com/006XXXXXXXXXXXXX`
- Direct ID: `006XXXXXXXXXXXXX` (15 or 18 character format)

## Logging

All operations are logged to:
- `sf_to_es.log` (single processing)
- `batch_sf_to_es.log` (batch processing)
- `interactive_sf_to_es.log` (interactive mode)

Log levels can be configured in `config.py`.

## Error Handling

The tool handles common scenarios:
- Invalid URLs
- Missing opportunities
- Authentication failures
- Network connectivity issues
- Elasticsearch indexing errors

## File Structure

```
├── sf_auth.py                    # Salesforce authentication module
├── sf_to_elasticsearch.py        # Single opportunity processor
├── batch_sf_to_elasticsearch.py  # Batch processor
├── interactive_sf_to_es.py       # Interactive interface
├── config.py                     # Configuration settings
├── README.md                     # This documentation
├── requirements.txt              # Python dependencies
└── examples/
    ├── sample_urls.txt           # Example URL file
    └── sample_output.json        # Example output format
```

## Elasticsearch Index Structure

The tool creates an index with this mapping:

```json
{
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
```

## Troubleshooting

### Authentication Issues
1. Ensure Salesforce CLI is installed: `sf --version`
2. Check authentication: `sf org list`
3. Re-authenticate if needed: `sf org login web -r https://elastic.my.salesforce.com`

### Elasticsearch Connection
1. Verify cluster URL format: `https://your-cluster.es.region.aws.elastic-cloud.com`
2. Check authentication credentials (username/password or API key)
3. Ensure cluster is accessible from your network
4. SSL verification is disabled, so certificate issues shouldn't occur

### Configuration Issues
1. Use environment variables for automation: `ES_CLUSTER_URL`, `ES_USERNAME`, etc.
2. API keys should be base64 encoded
3. Check that your user has index creation and document indexing permissions

### URL Extraction Failures
1. Use the interactive mode's URL tester
2. Check URL format against supported patterns
3. Ensure opportunity ID starts with '006'

### Performance Optimization
1. Use batch processing for multiple opportunities
2. Monitor Elasticsearch cluster resources
3. Adjust batch sizes in `config.py` if needed

## Security Considerations

- Salesforce credentials are managed by SF CLI
- Elasticsearch credentials are stored in config.py (consider environment variables for production)
- All communication uses HTTPS
- Opportunity IDs are used as document IDs to prevent duplicates

## Contributing

To extend functionality:
1. Add new fields to `FIELD_MAPPING` in config.py
2. Update Elasticsearch mapping if needed
3. Modify SOQL queries in processing scripts
4. Update documentation

## Support

For issues related to:
- **Salesforce authentication**: Check SF CLI documentation
- **Elasticsearch connectivity**: Verify cluster status and credentials
- **Data extraction**: Review Salesforce field permissions
- **Tool functionality**: Check log files for detailed error messages

## License

This tool is provided for internal use with Elastic's Salesforce and Elasticsearch infrastructure.
