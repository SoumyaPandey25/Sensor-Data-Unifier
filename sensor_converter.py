#!/usr/bin/env python3
"""
Sensor Data Converter

This utility converts and unifies sensor data from multiple JSON formats into a standardized structure.
"""

import json
import os
import sys
import logging
from datetime import datetime


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_data1_entry(entry):
    """
    Convert an entry from data-1.json format to the standardized format.
    
    Example input:
    {
        "id": "sensor-1",
        "value": 23.5,
        "timestamp": "2023-06-01T12:00:00Z"
    }
    
    Output:
    {
        "sensor": "sensor-1",
        "value": 23.5,
        "timestamp": 1685620800000  # milliseconds since epoch
    }
    """
    try:
        # Convert ISO timestamp (e.g., "2023-06-01T12:00:00Z") to milliseconds since epoch
        iso_time = entry.get('timestamp')
        if not iso_time:
            raise ValueError(f"Missing timestamp in entry: {entry}")
            
        dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%SZ")
        ms = int(dt.timestamp() * 1000)

        sensor_id = entry.get('id')
        if not sensor_id:
            raise ValueError(f"Missing sensor ID in entry: {entry}")
            
        sensor_value = entry.get('value')
        if sensor_value is None:  # Allow 0 as a valid value
            raise ValueError(f"Missing sensor value in entry: {entry}")
            
        return {
            "sensor": sensor_id,
            "value": sensor_value,
            "timestamp": ms
        }
    except (ValueError, KeyError) as e:
        logger.error(f"Error converting data-1 entry {entry}: {str(e)}")
        raise


def convert_data2_entry(entry):
    """
    Convert an entry from data-2.json format to the standardized format.
    
    Example input:
    {
        "deviceId": "device-2",
        "reading": 45.7,
        "time": 1685624400000
    }
    
    Output:
    {
        "sensor": "device-2",
        "value": 45.7,
        "timestamp": 1685624400000
    }
    """
    try:
        device_id = entry.get('deviceId')
        if not device_id:
            raise ValueError(f"Missing deviceId in entry: {entry}")
            
        reading = entry.get('reading')
        if reading is None:  # Allow 0 as a valid reading
            raise ValueError(f"Missing reading in entry: {entry}")
            
        time = entry.get('time')
        if time is None:
            raise ValueError(f"Missing time in entry: {entry}")
            
        return {
            "sensor": device_id,
            "value": reading,
            "timestamp": time
        }
    except (ValueError, KeyError) as e:
        logger.error(f"Error converting data-2 entry {entry}: {str(e)}")
        raise


def load_json_file(filename):
    """
    Load and parse data from a JSON file.
    
    Args:
        filename: Path to the JSON file
        
    Returns:
        Parsed JSON data as Python objects
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    try:
        logger.info(f"Loading data from {filename}")
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {filename}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filename}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading {filename}: {str(e)}")
        raise


def save_json_file(filename, data):
    """
    Save data to a JSON file.
    
    Args:
        filename: Path to save the JSON file
        data: Data to save as JSON
        
    Raises:
        PermissionError: If writing to the file is not permitted
        OSError: For other file system related errors
    """
    try:
        logger.info(f"Saving unified data to {filename}")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Successfully saved data to {filename}")
    except PermissionError:
        logger.error(f"Permission denied when writing to {filename}")
        raise
    except OSError as e:
        logger.error(f"Error saving data to {filename}: {str(e)}")
        raise


def convert_dataset(data, converter_func):
    """
    Convert an entire dataset using the specified converter function.
    
    Args:
        data: List of entries to convert
        converter_func: Function to use for conversion
        
    Returns:
        List of converted entries
    """
    result = []
    for i, entry in enumerate(data):
        try:
            converted = converter_func(entry)
            result.append(converted)
        except Exception as e:
            logger.warning(f"Skipping entry at index {i} due to error: {str(e)}")
    return result


def main():
    """
    Main function to run the sensor data conversion process.
    """
    try:
        # Define input and output file paths
        data1_path = "data-1.json"
        data2_path = "data-2.json"
        output_path = "output.json"
        
        # Check if input files exist
        if not os.path.exists(data1_path):
            logger.error(f"Input file not found: {data1_path}")
            sys.exit(1)
            
        if not os.path.exists(data2_path):
            logger.error(f"Input file not found: {data2_path}")
            sys.exit(1)
            
        # Load input data
        data1 = load_json_file(data1_path)
        data2 = load_json_file(data2_path)
        
        logger.info(f"Loaded {len(data1)} entries from {data1_path}")
        logger.info(f"Loaded {len(data2)} entries from {data2_path}")

        # Convert both datasets
        logger.info("Converting data-1 format entries")
        converted_data1 = convert_dataset(data1, convert_data1_entry)
        
        logger.info("Converting data-2 format entries")
        converted_data2 = convert_dataset(data2, convert_data2_entry)
        
        # Combine results
        result = converted_data1 + converted_data2
        
        logger.info(f"Combined dataset contains {len(result)} entries")
        
        # Sort by timestamp
        logger.info("Sorting combined dataset by timestamp")
        result.sort(key=lambda x: x['timestamp'])
        
        # Save unified result
        save_json_file(output_path, result)
        
        logger.info("Data conversion completed successfully")
        
    except Exception as e:
        logger.error(f"An error occurred during data conversion: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
