# Customer Matching Crawler

Automated candidate-to-job matching system built for a Berlin-based recruiting agency, integrated with Salesforce. The system crawls the career pages of contracted partner companies, matches open positions against candidate profiles using AI embeddings, and returns a ranked list of matches directly inside Salesforce.

This repository is part of a larger three-module project:

**Bestandskunden Matching Crawler** (this repo) - crawls career pages of 7,000+ partner companies, matches open positions against candidate profiles, returns ranked results in Salesforce.

## Tech Stack

- **Supabase** - database (Postgres) with `pgvector` for embeddings
- **Claude API (Anthropic)** - job structuring, CV parsing, match explanations
- **Voyage AI** - candidate and job embeddings
- **n8n** - workflow automation and orchestration (deployed on Contabo server)
- **Salesforce REST API** - candidate, account, and match record sync
- **SerpAPI** - Used to search the jobs on google
- **Playwright** - custom career page crawling

## Project Status

### Completed (Week 1)

- Supabase project set up with core tables: `companies`, `jobs`, `candidates`, `matches`, `crawl_logs`
- `pgvector` extension enabled for embeddings
- 200 contracted partner companies imported from CSV
- 3-layer ATS detection system built and run across all 200 companies
  - Results: 9 Personio, 3 Softgarden, 2 SuccessFactors, 1 Workday, 1 Teamtailor, 1 SmartRecruiters, 143 custom, 31 errors
- Personio and Softgarden adapters built and tested
- 49 real job postings extracted and structured via Claude
- Voyage AI embeddings generated for all jobs and 5 test candidates
- Basic matching engine running on pgvector cosine similarity
- n8n deployed with a basic workflow
- First matching results produced (e.g. candidate MCG-171885 matched at up to 61.3% score)

### Week 2: Salesforce Integration + Complete Candidate Profiles

- Connect to Salesforce sandbox via OAuth (External Client App)
- Pull candidate records (300+ structured skill fields) and company/account records via REST API
- Replace manual CSV import with live Salesforce sync
- Write match results back to Salesforce
- Parse candidate CVs (PDF) from Salesforce attachments and extract work history, employers, education, and additional skills via Claude
- Combine structured Salesforce fields with PDF content into a unified candidate profile
- Implement geocoding (Google Maps Geocoding OpenStreetMap Nominatim)
- Replace city-based filtering with precise distance calculation (PostGIS or Haversine)
- Replace n8n Schedule Trigger with a Webhook Trigger for a full end-to-end flow

### Week 3: Google Jobs + Scale + Lead Gen Foundation + Cost Analysis

- Integrate SerpAPI Google Jobs as a secondary job source, starting with the 31 companies that failed ATS detection
- Complete the generic Playwright crawler for all 143 custom career pages, with parallel processing, a queue system, and retry logic (target: 80%+ success rate)
- Add ATS adapters for Greenhouse, Lever, Workday, and others
- Filter out recruiting/staffing agencies from match results
- Deliver a cost analysis (low/high estimate) covering Claude API, Voyage AI, SerpAPI, scraping services, server, and Supabase costs for 30-50 users at 1,000 requests/month

### Scaling to 7,000-8,000 Partner Accounts

- ATS detection across all accounts
- Implement a priority queue (BullMQ or n8n queue): daily / every 3-4 days / weekly crawl tiers
- Cache ATS detection results (90 days) and deduplicate jobs
- Optimize Supabase indexing for large-scale queries

## Repository Structure

```
customer-matching-crawler/
├── data/
├── n8n-workflows/
├── salesforce-lwc/
├── scripts/
│   ├── api-cost-report.js
│   ├── backfill-locations-google.js
│   ├── build-skill-dictionary.js
│   ├── extract-location-from-description.js
│   ├── geocode-jobs.js
│   ├── reset-ats-data.js
│   ├── run-ats-detection.js
│   ├── run-ats-only.sh
│   ├── run-candidate-embeddings-queue.js
│   ├── run-embeddings-queue.js
│   ├── run-embeddings.js
│   ├── run-google-jobs-only.sh
│   ├── run-google-jobs.js
│   ├── run-job-structuring-queue.js
│   ├── run-job-structuring-worker.js
│   ├── run-job-structuring.js
│   ├── run-join.js
│   ├── run-match.js
│   ├── run-matching.js
│   ├── run-onapply.js
│   ├── run-personio.js
│   ├── run-pipeline-jobs-only.sh
│   ├── run-recruitee.js
│   ├── run-rexx.js
│   ├── run-smartrecruiters.js
│   ├── run-softgarden.js
│   ├── run-successfactors.js
│   ├── run-teamtailor.js
│   ├── run-umantis.js
│   ├── run-workday.js
│   ├── run-workwise.js
│   ├── seed-candidates.js
│   └── test-connection.js
├── src/
│   ├── ai/
│   │   ├── generate-embeddings.js
│   │   ├── job-structurer.js
│   │   └── skills-dictionary.json
│   ├── ats-adapters/
│   │   ├── ats-detector.js
│   │   ├── personio-adapter.js
│   │   └── softgarden-adapter.js
│   ├── crawlers/
│   │   ├── custom-crawler-queue.js
│   │   └── softgarden-crawler-queue.js
│   ├── matching/
│   │   ├── match-engine.js
│   │   └── server.js
│   ├── salesforce/
│   └── utils/
├── supabase/
├── .env.example
├── package.json
└── scheduler.js
```

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/DevFrontai678/customer-matching-crawler.git
   cd customer-matching-crawler
   ```
2. Install dependencies:
   ```bash
   npm install
   ```
3. Copy `.env.example` to `.env` and fill in the required credentials (Supabase, Claude API, Voyage AI, Salesforce, n8n).
   ```bash
   cp .env.example .env
   ```
4. Test the Supabase connection:
   ```bash
   node scripts/test-connection.js
   ```
5. Run ATS detection on the seeded companies:
   ```bash
   node scripts/run-ats-detection.js
   ```
6. Generate embeddings and run the matching engine:
   ```bash
   node scripts/run-embeddings.js
   node scripts/run-matching.js
   ```
## n8n Workflow

Part 1 – Data Intake & CV Processing
1. Webhook triggers the workflow when a request comes in.
2. Get an account and Get a row fetch the relevant Salesforce account/ supabase candidate record.
3. Candidate Google Job Search (a new workflow call using the webhook that retrieves matching jobs, store in supabase GOOGLE JOB SEARCH TABLE and sends them via email).
4. An If node checks a condition and branches into for the candidate:
 - Delete Previous Matches and Delete Previous Candidate Data (clears old match/candidate records before regenerating)
5. Both branches merge into Get Attachments, which checks if a CV file exists.
6. CV attached? decision:
 - If yes: Get CV ID → Download PDFs → Extract from File → Parse CV's Based on MCQ → Format Results
 - If no: Formatting Results Without CV
7. Both paths go into Merge, then JSON Format to standardize the output.

Part 2 – Matching & Storage
8. Add Candidates Data saves candidate info into Supabase (create row) candidate table.
9. Embedding & Matches generates embeddings and finds job matches and then store them into matches table into Supabase.
10. Split Matches Results breaks results into individual item.
11. Go to Customer Job Advert Crawl Detail (a sub-workflow that check the company status which is crawled or not if not it will create a new crawl details for the relevant company).
12. Another workflow call for checking the skill fields which are matched with the candidate and salesforce skills. After matching, common skills will be stored in (Candidate Salesforce Matched skills) table in Supabase.
13. Matches then get Summarized and Combined, feeding into Candidate Job Matching Profile in salesforce.
14. Aggregate compiles everything, then Matches - Done sends a notification/message.
15. Finally, Respond to Webhook sends the response back to whoever triggered the workflow.

## Notes

- Data flows: Salesforce (candidates/accounts) → Supabase (structured storage) → Claude/Voyage AI (structuring + embeddings) → matching engine (pgvector) → results written back to Salesforce.
- n8n orchestrates the end-to-end pipeline and is deployed on a Contabo server.
- This project is designed from the start to scale to 7,000-8,000 partner accounts, not just the current 200-account pilot.
