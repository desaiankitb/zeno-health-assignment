# 🛒 E-Commerce Analytics Dashboard

> **Advanced analytics and predictive modeling for Brazilian e-commerce data using Apache Superset, PostgreSQL, and Python**

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-green.svg)](https://postgresql.org)
[![Apache Superset](https://img.shields.io/badge/Apache%20Superset-Latest-orange.svg)](https://superset.apache.org)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 📊 Overview

This project provides basic analytics and predictive insights for Brazilian e-commerce data from Olist. It combines data engineering, business intelligence, and machine learning to deliver actionable insights for improving customer experience, delivery performance, and business growth.

### 🎯 Key Features

- **📈 Dashboards**: Multi-dimensional analytics with Apache Superset
- **🔍 Delivery Performance Analysis**: Identify late delivery patterns by product category
- **👥 Customer Lifetime Value (LTV) Analysis**: RFM clustering for customer segmentation
- **🤖 Predictive Modeling**: Machine learning models for business forecasting


## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 13+
- Apache Superset
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd zeno-health-assignment
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

4. **Configure PostgreSQL**
   ```bash
   # Create database and user
   createdb olist_analytics
   ```

5. **Load data into PostgreSQL**
   ```bash
   python utils/load_data.py
   ```

6. **Set up Apache Superset**
   ```bash
   # Follow Apache Superset installation guide
   # Import the dashboard from dashboard/dashboard_export_20250818T104127.zip
   ```

## 📊 Analysis Insights

# Actionable Insignts: 
1. Prioritize process improvements in categories with the highest number of late orders (e.g., Electronics). Reduce total late orders in Electronics by X% as a target for the quarter. 
2. Investigate and resolve issues in categories with the highest late delivery rates (e.g., categories with >Y% late). Reduce late delivery rate in these categories to below Z as a target for the quarter. 

### 👥 Customer Lifetime Value (LTV) Analysis

**Key Findings:**
- "Low Value" cluster actually has highest average LTV (330 BRL)
- High-frequency customers don't always correlate with high monetary value
- Customer segmentation reveals hidden revenue opportunities

**Actionable Insignt:** 
1. Allocate more marketing budget to customers in the top LTV cluster ("Low Value"). Increase repeat purchase rate in this segment by X% as a target for the coming quarter. Provide them with coupons and discounts to bring them back to the platform and make them buy more often. 

### 👥 Predictive Analysis -- At risk customers 

**Approach tried:**
1. logistic regression with simple feature engineering without SMOTE ❌  
2. logistic regression with simple feature engineering with SMOTE ❌ 
3. logistic regression with detailed feature engineering with SMOTE ✅ 
4. xgboost with simple feature engineering with SMOTE ❌ 
5. xgboost with detailed feature engineering with SMOTE ✅ ✅ 

## 🔧 Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=olist_analytics
```

### Database Schema

The data loader creates the following tables:
- `olist_customers_dataset`
- `olist_geolocation_dataset`
- `olist_order_items_dataset`
- `olist_order_payments_dataset`
- `olist_order_reviews_dataset`
- `olist_orders_dataset`
- `olist_products_dataset`
- `olist_sellers_dataset`

### Accessing Dashboards

1. Open Apache Superset
2. Import dashboard from `dashboard/dashboard_export_20250818T104127.zip`
3. Configure database connection
4. Explore interactive visualizations 
5. Screenshots of the dashboards are kept under `dashboard/` folder.

## 🤖 Machine Learning Features

The project includes predictive modeling capabilities:

- **Customer Churn Prediction**: Identify at-risk customers
- **Delivery Time Forecasting**: Predict delivery delays
- **Revenue Forecasting**: Project future sales
- **Customer Segmentation**: Advanced clustering algorithms

## 📊 Dashboard Features

### Available Dashboards

1. **Customer & Session Analysis**: Customer behavior and engagement metrics
2. **Product & Category Analysis**: Product performance and category insights
3. **Top-line Metrics**: Key business performance indicators

### Key Visualizations

- Delivery performance by product category
- Customer lifetime value clustering
- Geographic distribution of orders
- Payment method analysis
- Review sentiment analysis

## 🚀 Production Deployment

### Docker Setup

1. Setup a docker for DB 
2. Data Ingestion pipeline (Optionally, use Airflow)
3. Set up a docker for Apache Superset `->` Connect DB `->` Load dashboard 
4. Write a FastAPI (or similar) for model inference. 

## Future Improvements

- **Use of Pydantic**: Use Pydantic for database schema read/write.
- **Database tuning**: Add indexes on join/filter columns; 
- **Parallelism upgrade**: Offer process-based parallelism (multiprocessing) for CPU-heavy parsing; cap DB connections via pool.
- **Superset hardening**: Add more filters, drilldowns, dashboard auto-refresh, and embedding options.
- **ML ops**: Add MLflow for experiment tracking and model registry; optional AutoML (or AutoKeras) baseline for quick model selection.
- **ML**: More robust feature engineering can be done current model depicts the baseline level performance.

## 🙏 Acknowledgments

- **Olist**: For providing the Brazilian e-commerce dataset
- **Apache Superset**: For the powerful BI platform