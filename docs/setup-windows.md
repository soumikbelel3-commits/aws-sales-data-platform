# Setup guide (Windows)

## 1. Secure the AWS account (Phase 0)

- [ ] Find out how you sign in: top-right account menu in the AWS Console shows Root user, IAM user, or SSO.
- [ ] Turn on MFA for the root user (IAM console → Security credentials → Assign MFA device).
- [ ] Create a day-to-day identity (IAM Identity Center user, or an IAM user with MFA). Stop using root for daily work.
- [ ] Billing and Cost Management → Budgets → create a small monthly cost budget with email alerts
      for actual and forecasted spend.
- [ ] Pick one region and use it everywhere (e.g. `ap-south-1` Mumbai).

## 2. Install tools

| Tool | Check |
|------|-------|
| Python 3.11+ | `python --version` |
| Git | `git --version` |
| VS Code | `code --version` |
| AWS CLI v2 | `aws --version` |

## 3. Configure AWS CLI credentials

Preferred (IAM Identity Center / SSO):

```powershell
aws configure sso --profile sales-dev
aws sso login --profile sales-dev
aws sts get-caller-identity --profile sales-dev
```

IAM user with access keys (only if SSO is not available):

```powershell
aws configure --profile sales-dev
```

Keys are stored in `%USERPROFILE%\.aws\` — never copy them into this repo.

## 4. Project environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1      # if blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
pip install -r requirements-dev.txt
pytest
```

## 5. Create the S3 bucket (Phase 1)

Console: S3 → Create bucket

- Name: `<yourname>-sales-data-platform-dev-<random>` (must be globally unique)
- Region: the one you chose
- Block all public access: **on**
- Default encryption: **on** (SSE-S3)

Or with the CLI:

```powershell
aws s3api create-bucket --bucket <bucket> --region ap-south-1 --create-bucket-configuration LocationConstraint=ap-south-1 --profile sales-dev
aws s3api put-public-access-block --bucket <bucket> --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true --profile sales-dev
```

## 6. Generate and upload data

```powershell
python src/generate_data.py
python src/upload_to_s3.py --bucket <bucket> --profile sales-dev
aws s3 ls s3://<bucket>/raw/sales/ --recursive --profile sales-dev
```

## Clean up after each session

- Delete Kinesis streams, Firehose delivery streams, and Redshift resources you are not using.
- Check the Billing dashboard.
