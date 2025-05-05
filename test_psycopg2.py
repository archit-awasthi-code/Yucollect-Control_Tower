import os
print("Current environment:")
print(f"DYLD_LIBRARY_PATH: {os.environ.get('DYLD_LIBRARY_PATH')}")
print(f"DYLD_FALLBACK_LIBRARY_PATH: {os.environ.get('DYLD_FALLBACK_LIBRARY_PATH')}")
print(f"PATH: {os.environ.get('PATH')}")
print("\nTrying to import psycopg2...")

try:
    import psycopg2
    print("Successfully imported psycopg2")
    print(f"psycopg2 version: {psycopg2.__version__}")
except Exception as e:
    print(f"Error importing psycopg2: {e}")
    import sys
    print(f"\nPython version: {sys.version}")
    print(f"Python path: {sys.path}")
