import pandas as pd
from typing import Dict, Tuple


class DataQualityChecker:
    """Data Quality Checker for DataFrame and Dictionary validation"""
    
    def __init__(self, data: pd.DataFrame = None):
        """Initialize with optional DataFrame"""
        self.data = data
        self.issues = []

    def check_missing_values(self, threshold: float = 0.5) -> Dict:
        """Check for missing values in each column and return the percentage of missing values."""
        if self.data is None:
            return {"passed": False, "message": "No data provided"}
            
        missing_values = self.data.isnull().sum() / len(self.data)
        problematic_columns = missing_values[missing_values > threshold]

        output = {
            "passed": len(problematic_columns) == 0,
            "columns_with_issues": problematic_columns.to_dict(),
            "message": f"Columns with more than {threshold*100}% missing values: {list(problematic_columns.index)}"
        }

        if not output["passed"]:
            self.issues.append(output["message"])

        return output

    def check_duplicates(self) -> Dict:
        """Check for duplicate rows in the DataFrame and return the count of duplicates."""
        if self.data is None:
            return {"passed": False, "message": "No data provided"}
            
        duplicate_count = self.data.duplicated().sum()

        output = {
            "passed": duplicate_count == 0,
            "duplicate_rows": duplicate_count,
            "message": f"Number of duplicate rows found: {duplicate_count}"
        }
        
        if not output["passed"]:
            self.issues.append(output["message"])

        return output

    def check_data_types(self, expected_types: Dict[str, str]) -> Dict:
        """Check the data types of each column and return a dictionary of mismatches."""
        if self.data is None:
            return {"passed": False, "message": "No data provided"}
            
        mismatches = {}
        for col, expected_type in expected_types.items():
            if col not in self.data.columns:
                mismatches[col] = f"Column {col} not found"
            else:
                actual_type = str(self.data[col].dtype)
                # Handle tuple of acceptable types
                if isinstance(expected_type, tuple):
                    if actual_type not in expected_type:
                        mismatches[col] = f"Expected one of {expected_type}, found {actual_type}"
                else:
                    if actual_type != expected_type:
                        mismatches[col] = f"Expected {expected_type}, found {actual_type}"

        output = {
            "passed": len(mismatches) == 0,
            "type_mismatches": mismatches,
            "message": f"Data type mismatches found: {len(mismatches)}"
        }

        if not output["passed"]:
            self.issues.append(output["message"])

        return output

    def check_empty_dataset(self) -> Dict:
        """Check if the dataset is empty."""
        if self.data is None:
            return {"passed": False, "row_count": 0, "message": "No data provided"}
            
        is_empty = len(self.data) == 0

        output = {
            "passed": not is_empty,
            "row_count": len(self.data),
            "message": "Dataset is empty" if is_empty else "Dataset is not empty"
        }

        if not output["passed"]:
            self.issues.append(output["message"])

        return output
    
    def run_all_checks(self, expected_types: Dict[str, str] = None, missing_value_threshold: float = 0.5) -> Dict:
        """Run all data quality checks and return a summary."""
        output = {   
            "empty_dataset": self.check_empty_dataset(),
            "missing_values": self.check_missing_values(threshold=missing_value_threshold),
            "duplicates": self.check_duplicates(),
        }
        
        if expected_types:  
            output["data_types"] = self.check_data_types(expected_types)

        all_passed = all(check["passed"] for check in output.values())
            
        return {
            "validation_passed": all_passed,
            "issues_found": self.issues,
            "detailed_results": output
        }

    def validate_dict(self, data_dict: Dict, schema: Dict) -> Tuple[bool, Dict]:
        """
        Validate a dictionary against a schema.
        
        Args:
            data_dict: Dictionary to validate
            schema: Expected schema {field_name: expected_type(s)}
            
        Returns:
            Tuple of (is_valid, details)
        """
        errors = {}
        
        for field, expected_type in schema.items():
            # Check if field exists
            if field not in data_dict:
                errors[field] = f"Missing required field: {field}"
                continue
            
            value = data_dict[field]
            
            # Check type (handle tuple of types)
            if isinstance(expected_type, tuple):
                if not isinstance(value, expected_type):
                    errors[field] = f"Expected type {expected_type}, got {type(value).__name__}"
            else:
                if not isinstance(value, expected_type):
                    errors[field] = f"Expected type {expected_type.__name__}, got {type(value).__name__}"
        
        is_valid = len(errors) == 0
        
        details = {
            "errors": errors if errors else None,
            "fields_validated": len(schema),
            "fields_passed": len(schema) - len(errors)
        }
        
        return is_valid, details

# Example usage
if __name__ == "__main__":
    print("="*70)
    print("DATA QUALITY CHECKER - TEST")
    print("="*70)
    
    # Test 1: DataFrame validation
    print("\n1. Testing DataFrame validation...")
    test_df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'score': [85.5, 90.0, 88.5]
    })
    
    checker = DataQualityChecker(test_df)
    
    expected_types = {
        'name': 'object',
        'age': 'int64',
        'score': 'float64'
    }
    
    result = checker.run_all_checks(expected_types)
    print(f"   DataFrame validation: {'PASSED' if result['validation_passed'] else 'FAILED'}")
    
    # Test 2: Dictionary validation (for API responses)
    print("\n2. Testing Dictionary validation (API response)...")
    test_dict = {
        'city': 'New York',
        'temperature': 25.5,
        'humidity': 60,
        'pressure': 1013,
        'timestamp': '2026-01-04',
        'source': 'weather_api'
    }
    
    schema = {
        'city': str,
        'temperature': (int, float),
        'humidity': int,
        'pressure': int,
        'timestamp': str,
        'source': str
    }
    
    checker2 = DataQualityChecker()
    is_valid, details = checker2.validate_dict(test_dict, schema)
    print(f"   Dictionary validation: {'PASSED' if is_valid else 'FAILED'}")
    print(f"   Fields validated: {details['fields_passed']}/{details['fields_validated']}")
    
    print("\n" + "="*70)
    print("ALL TESTS COMPLETE")
    print("="*70)