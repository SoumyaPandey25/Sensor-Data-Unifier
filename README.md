# Sensor-Data-Unifier

A Python utility that converts and unifies sensor data from multiple JSON formats into a standardized structure.

## Overview

This tool reads sensor data from two different JSON file formats, converts them to a standardized format, and outputs a single unified JSON file with all entries sorted by timestamp.

## Features

- Reads and parses data from two different JSON file formats
- Converts both data formats to a unified structure with standardized fields
- Handles timestamp conversion from ISO format to milliseconds
- Sorts the combined data by timestamp
- Outputs the unified data to a single JSON file
- Includes error handling for file operations and data validation

## Requirements

- Python 3.6 or higher
- Standard library modules: json, datetime, os, sys, logging

## Usage

1. Place input files in the same directory as the script:
   - `data-1.json`
   - `data-2.json`

2. Run the script:
   
