# Overview

This project is a comprehensive CRM synchronization system that continuously syncs data between Kommo CRM and a Supabase database. The system is designed for real estate brokers and includes advanced gamification features to track performance metrics. It handles multiple companies simultaneously with intelligent rate limiting, automatic retry mechanisms, and real-time analytics.

The system processes brokers, leads, activities, and pipeline stages while calculating performance scores based on configurable rules. It's built as a Flask API with continuous background synchronization workers that respect Kommo's API rate limits (7 requests/second).

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Backend Architecture
- **Flask REST API**: Main application server exposing sync control endpoints
- **Background Workers**: Threading-based continuous sync workers for each company
- **Rate Limiting**: Intelligent rate limiting system respecting Kommo API constraints (7 req/s)
- **Retry Logic**: Exponential backoff with configurable retry attempts for failed requests

## Data Synchronization
- **Incremental Sync**: Only processes new or changed data to optimize performance
- **Multi-company Support**: Parallel synchronization for multiple CRM instances
- **Data Types**: Syncs brokers, leads, activities, and pipeline stages
- **Change Detection**: Hash-based comparison to identify data modifications

## Gamification Engine
- **Performance Scoring**: Automatic calculation of broker performance metrics
- **Configurable Rules**: Flexible point assignment based on activities (visits, proposals, sales)
- **Real-time Updates**: Dynamic recalculation of scores during sync operations

## Error Handling & Monitoring
- **File-based Logging**: Comprehensive logging system with automatic log rotation
- **Health Monitoring**: Continuous monitoring of sync worker health
- **Resilient Architecture**: Automatic recovery from API failures and network issues

## Data Processing
- **Pandas Integration**: Data manipulation and analysis using pandas DataFrames
- **Timezone Handling**: Proper timezone conversion for São Paulo timezone
- **Batch Processing**: Configurable batch sizes for database operations

# External Dependencies

## Primary Services
- **Kommo CRM API**: Source system for broker and lead data
- **Supabase**: PostgreSQL database with real-time capabilities for data storage
- **Supabase Auth**: Authentication and authorization system

## Python Libraries
- **Flask**: Web framework for REST API
- **Pandas**: Data manipulation and analysis
- **SQLAlchemy**: Database ORM and connection management
- **Requests**: HTTP client for API communication
- **Threading**: Concurrent execution for multi-company sync

## Database Schema
- **kommo_config**: Company configuration and API credentials
- **brokers**: Real estate broker information and performance data
- **leads**: Lead data and pipeline information

# Installation

1. **Clone the repository:**