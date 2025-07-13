# Watchtower - Databricks Logging Solution

A solution accelerator to standardizing, centralizing, and ingesting Databricks cluster logs
for enhanced log searching, troubleshooting, and optimization.

The goal of this repo is to provide platform adminsitrators and data engineers a reference implementation
for scalable log ingestion, as well as broader guidance for practioners to use logging in jobs.

![Log Search dashboard](./images/01-dashboard-logsearch.png)

![Top N analysis dashboard](./images/02-dashboard-top-n.png)

## Quick Start

1. **Prerequisites**
   ```bash
   pip install databricks-cli
   ```

2. **Configure Databricks**
   ```bash
   # Option A: Use databricks configure (interactive)
   databricks configure
   
   # Option B: Use environment file (recommended for CI/CD)
   cp env.example .env
   # Edit .env with your Databricks workspace URL, token, and warehouse ID
   # Get warehouse ID from: Databricks → SQL Warehouses → Copy warehouse ID
   ```

3. **Deploy Everything**
   ```bash
   ./scripts/deploy.sh
   ```

4. **Clean Up When Done**
   ```bash
   ./scripts/cleanup.sh
   ```

## CI/CD Setup (Optional)

To enable automatic testing on Pull Requests:

1. **Add GitHub Repository Secrets**:
   - Go to your repo → Settings → Secrets and variables → Actions
   - Add secret: `DATABRICKS_TOKEN` (your Databricks token)
   - Optionally add variable: `DATABRICKS_HOST` (defaults to `https://e2-demo-field-eng.cloud.databricks.com/`)

2. **What happens automatically**:
   - **Pull Requests**: Validated and tested with isolated workspace paths
   - **Main branch**: Deployed to your dev environment
   - **PR cleanup**: Resources automatically cleaned up when PR is closed

## What Gets Deployed

- **Pipeline**: `Databricks log ingestion pipeline` 
- **Workflow**: `watchtower_demo_job`
- **Dashboard**: `Logs Dashboard` (deployed alongside pipeline)
- **Location**: `/Workspace/Users/your-email@company.com/.bundle/watchtower/`

## Manual Commands (if you prefer)

```bash
cd terraform && terraform init && terraform apply && cd .. # Deploy Catalog resources and init scripts
databricks bundle validate    # Check configuration
databricks bundle deploy      # Deploy to workspace
databricks bundle run demo_workflow # Run the demo workflow
databricks bundle summary     # See what's deployed
databricks bundle destroy     # Remove everything
cd terraform && terraform destroy && cd ..
```

## Customizing for Your Project

1. Update `databricks.yml` or the `resources/*.yml` files with your job/pipeline settings
2. Modify `notebooks/dlt_pipeline.ipynb` to customize the log ingestion pipeline
3. Modify the workspace `host` and `root_path` as needed
4. Modify `terraform/main.tf` as needed, or create a `terraform/.auto.tfvars` file to override Terraform variables.

## Project Structure

```
├── databricks.yml           # Main DABs configuration
├── notebooks/
│   ├── dlt_pipeline.ipynb   # Main watchtower log ingestion pipeline
│   └── demo_etl.ipynb       # Notebook for the demo job
├── dashboards/
│   └── watchtower.lvdash.json    # Main watchtower dashboard for log analysis
├── resources/                    # DAB resources to deploy
│   ├── watchtower.dashboard.yml  # Main watchtower dashboard for log analysis
│   ├── watchtower.demo.job.yml   # Demo job using structured logging frameworks
│   └── watchtower.pipeline.yml   # Main pipeline for ingesting logs
├── terraform/
│   └── init-scripts/
│       └── configure_log4j.sh  # Modifies Spark driver log4j2.xml to use JSON format
└── scripts/
    ├── deploy.sh           # Automated deployment wrapper
    └── cleanup.sh          # Automated cleanup wrapper
```

That's it! 🚀 