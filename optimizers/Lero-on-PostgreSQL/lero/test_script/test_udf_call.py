#!/usr/bin/env python3

import time
import sys

# This script leverages the existing SQLAlchemy engine and restart function
# from your utils.py file to provide a focused test.
try:
    # We are importing the function we want to test directly from your framework.
    from utils import clear_cache
    print("Successfully imported the 'clear_cache' function from utils.py.")
except ImportError as e:
    print(f"ERROR: Could not import from 'utils.py'.\n  -> {e}")
    print("Please ensure this script is in the same directory as utils.py.")
    sys.exit(1)

def main():
    """
    Main function to execute the UDF test.
    """
    print("="*60)
    print("UDF Execution Test Script")
    print("="*60)
    print("This script will make a single call to the `clear_cache()` function,")
    print("which in turn calls the `clear_cache()` UDF on the remote database.")
    print("\nYour goal is to verify if the UDF is being triggered correctly.")
    print("\nINSTRUCTIONS:")
    print("1. On the REMOTE DATABASE SERVER, run the following command in a terminal:")
    print("   tail -f /app/installation_scripts/udf_log.txt")
    print("2. This script will count down from 5 and then make the call.")
    print("3. If the call is successful, you should see new lines appear in the log file.")
    print("-" * 60)

    for i in range(5, 0, -1):
        print(f"Calling the UDF in {i}...", end='\r')
        time.sleep(1)
    
    print("\nMaking the call now...")
    
    # Execute the function we imported from utils.
    # This function contains the full logic: trigger, expect disconnection, and poll.
    clear_cache()
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)
    print("The `clear_cache` function has finished executing.")
    print("Please check the output of your `tail -f` command on the server.")
    print("\nIf new log entries appeared, the UDF was triggered successfully,")
    print("and the problem lies in the interaction with the larger training script.")
    print("\nIf NO new entries appeared, the problem is with the UDF call itself,")
    print("even in this simple case (likely permissions or a transactional hang).")

if __name__ == "__main__":
    main()