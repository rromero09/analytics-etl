# Sales ETL Pipeline
## Intented for Square API v2

An automated ETL pipeline that fetches sales data from Square POS API, transforms it, and loads it into PostgreSQL for business analytics.

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688.svg)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791.svg)](https://www.postgresql.org/)

---

## 🎯 Overview

This project automates the monthly collection and processing of sales data from 3 Square POS locations. The ETL pipeline runs on GitHub Actions, fetching transaction data, transforming it into a normalized format, and loading it into a partitioned PostgreSQL database hosted on AWS RDS.

**Purpose**: Enable business intelligence and data visualization in tools like Tableau by maintaining a clean, queryable sales database.

---

## 🏗 Architecture

### Data Flow

```
Square POS API (3 Locations)
         ↓
   [GitHub Actions]
   Monthly Trigger (1st @ 3 AM CT)
         ↓
   ETL Pipeline
   ├─ Fetch: Square Orders API
   ├─ Transform: JSON → Database Schema
   └─ Load: Bulk Insert to PostgreSQL
         ↓
   AWS RDS PostgreSQL
   ├─ locations (reference)
   └─ sales (partitioned by location)
         ↓
   Tableau / BI Tools
```

### Key Components

1. **Square Service** (`app/services/square_service.py`)
   - Authenticates with Square API
   - Fetches completed orders with pagination
   - Handles rate limiting and retries

2. **ETL Service** (`app/services/etl_service.py`)
   - Transforms Square JSON to database schema
   - Converts timestamps (UTC → America/Chicago)
   - Extracts date components (month, day_of_week)
   - Validates data before insertion

3. **Database Service** (`app/services/database_service.py`)
   - Manages PostgreSQL connections
   - Performs bulk inserts
   - Queries location mappings
   - Handles transactions and rollbacks

4. **Monthly ETL Script** (`app/scripts/monthly_etl.py`)
   - Orchestrates the complete ETL process
   - Processes all 3 locations
   - Logs progress and statistics
   - Runs via GitHub Actions

---

## 🛠 Tech Stack

- **Python 3.11**: Core language
- **FastAPI**: REST API framework
- **PostgreSQL 15**: Relational database with partitioning
- **AWS RDS**: Managed database hosting (Free Tierfor for now)
- **Square API v2**: Orders API for transaction data
- **GitHub Actions**: CI/CD automation
- **SQLAlchemy**: ORM for database operations

---

## 📁 Project Structure

```
analytics-etl/
├── app/
│   ├── services/
│   │   ├── square_service.py      # Square API integration
│   │   ├── etl_service.py         # Data transformation logic
│   │   └── database_service.py    # PostgreSQL operations
│   ├── utils/
│   │   ├── config.py              # Environment configuration
│   │   └── date_utils.py          # Timezone helpers
│   └── scripts/
│       └── monthly_etl.py         # ETL orchestration
├── .github/
│   └── workflows/
│       └── monthly_etl.yml        # Automation workflow
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🗄 Database Schema

### Locations Table (Reference)
```sql
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    square_id TEXT UNIQUE NOT NULL
);
```

Stores the 3 physical locations and their Square IDs.

### Sales Table (Partitioned)
```sql
CREATE TABLE sales (
    sale_id BIGSERIAL,
    item_name TEXT NOT NULL,
    sale_price NUMERIC(12,2) NOT NULL,  -- Base price (excludes tax)
    qty INT NOT NULL,
    sale_timestamp TIMESTAMPTZ NOT NULL, -- Chicago timezone
    month TEXT NOT NULL,                 -- YYYY-MM format
    day_of_week TEXT NOT NULL,           -- Monday, Tuesday, etc.
    item_category TEXT,
    location_id INT NOT NULL,
    
    PRIMARY KEY (sale_id, location_id),
    FOREIGN KEY (location_id) REFERENCES locations(id)
) PARTITION BY LIST (location_id);
```

**Partitioning**: One partition per location (`sales_location_1`, `sales_location_2`, `sales_location_3`) for optimized queries.

**Indexes**:
- `idx_sales_timestamp` - Date range queries
- `idx_sales_month` - Monthly aggregations
- `idx_sales_item_name` - Product analysis

---

## 🔄 Data Transformation

### Square API → Database Mapping

| Square Field | Database Column | Transformation |
|--------------|----------------|----------------|
| `line_item.name` | `item_name` | Direct mapping |
| `line_item.base_price_money.amount` | `sale_price` | Divide by 100 (cents → dollars) |
| `line_item.quantity` | `qty` | Direct mapping |
| `sale_timestamp` | UTC → America/Chicago |
| Extract from timestamp | `month` | Format as 'YYYY-MM' |
| Extract from timestamp | `day_of_week` | 'Monday', 'Tuesday', etc. |
| `line_item.variation_name` | `item_category` | Direct mapping or 'N/A' |
| Lookup from locations | `location_id` | Join on square_id |

### Important Notes

- **Pricing**: Only `base_price_money` is stored. Taxes, tips, and service charges are excluded.
- **Timezone**: All timestamps are stored in America/Chicago (handles CST/CDT automatically).
- **Granularity**: Each line item in an order becomes a separate database row.
- **Order State**: Only `COMPLETED` orders are fetched.

---

## 🚀 Setup & Configuration

### Prerequisites

- Python 3.11+
- Square Developer Account with API access token
- AWS account with RDS PostgreSQL instance
- GitHub repository with Actions enabled

### Installation

```bash
# Clone repository
git clone https://github.com/rromero09/analytics-etl.git
cd analytics-etl

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials
```

### Environment Variables

```bash
# Database Configuration (Production)
PROD_DB_HOST=your-rds-endpoint_here
PROD_DB_PORT=5432 #default port for postgresql
PROD_DB_NAME=db_name_here
PROD_DB_USER=github_actions_user
PROD_DB_PASSWORD=your_secure_password

# Square API
SQUARE_ACCESS_TOKEN= your_token_here

# Application
ENVIRONMENT=production  # or 'development' for local
LOG_LEVEL=INFO
```

---

## 🤖 GitHub Actions Automation

### Automated Schedule

The ETL pipeline runs automatically **on the 1st of each month at 3 AM Chicago time**.

### Workflow Configuration

Required GitHub Secrets (Settings → Secrets and variables → Actions):

| Secret | Description |
|--------|-------------|
| `AWS_ROLE_ARN` | IAM Role ARN for OIDC authentication |
| `AWS_REGION` | AWS region|
| `DB_HOST` | RDS endpoint |
| `DB_NAME` | Database name |
| `DB_USER` | Database username |
| `DB_PASSWORD` | Database password |
| `SQUARE_ACCESS_TOKEN` | Square API token |

### Manual Trigger

1. Go to: `GitHub Repo → Actions → Monthly Sales ETL`
2. Click **"Run workflow"**
3. Options:
   - **Run mode**: `production` (all data) or `test` (200 orders)
   - **Location**: `all` or specific location (1, 2, 3)
   - **Dates**: Leave empty for previous month

### Workflow Steps

1. **Checkout code** - Clone repository
2. **Setup Python** - Install Python 3.11
3. **Install dependencies** - Install requirements.txt
4. **Configure AWS credentials** - OIDC authentication
5. **Test database connection** - Verify connectivity
6. **Run ETL** - Execute monthly_etl.py
7. **Summary** - Report statistics

---

## 📊 Monitoring

### GitHub Actions Logs

View execution details:
- Go to `Actions` tab in repository
- Click on workflow run
- View logs for each step

### Email Notifications

- Automatic emails on workflow failure
- Configure in `GitHub Settings → Notifications → Actions`

### Database Verification

```sql
-- Check recent sales
SELECT * FROM sales 
ORDER BY sale_timestamp DESC 
LIMIT 10;

-- Monthly summary
SELECT 
    month, 
    location_id, 
    COUNT(*) as order_count,
    SUM(sale_price * qty) as total_revenue
FROM sales
GROUP BY month, location_id
ORDER BY month DESC;
```

---

## 🔧 AWS Infrastructure

### RDS PostgreSQL Setup

- **Instance**: db.t3.micro (Free Tier)
- **Region**: us-east-2
- **Storage**: 20 GB (Free Tier)
- **Backups**: 7-day retention (automated)

### IAM Authentication

- **Method**: OpenID Connect (OIDC)
- **Benefits**: No long-lived credentials, automatic token rotation
- **Setup**: IAM Identity Provider + Role with RDS permissions

### Security

- Security group allows GitHub Actions IPs
- IAM role-based access
- Encrypted connections (SSL/TLS)

---

## 📈 Performance

- **Execution Time**: ~30-60 seconds per month (production)
- **Test Mode**: ~5-10 seconds (200 orders)
- **API Requests**: ~15-20 per location per month
- **Database Rows**: ~3,000-10,000 per month (all locations)
- **GitHub Actions Cost**: $0.00 (within free tier)

---

## 🔍 Key Features

✅ **Automated**: Runs monthly without manual intervention  
✅ **Multi-Location**: Processes 3 locations in parallel  
✅ **Timezone-Aware**: Handles CST/CDT conversions  
✅ **Partitioned**: Location-based partitioning for fast queries  
✅ **Reliable**: Retry logic, error handling, rollback on failure  
✅ **Observable**: Detailed logs and statistics  
✅ **Secure**: AWS OIDC, no hardcoded credentials  

---

## 📝 License

MIT License - See LICENSE file for details.

---

## 👤 Author

**Rafael Romero**  
[GitHub](https://github.com/rromero09)
