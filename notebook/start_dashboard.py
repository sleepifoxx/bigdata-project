"""
Launcher for Streamlit Dashboard
"""
import os
import sys

# Set Streamlit config to avoid email prompt
os.environ['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'

if __name__ == "__main__":
    print("="*80)
    print("🚀 Starting Streamlit Dashboard")
    print("="*80)
    print("Dashboard URL: http://localhost:8501")
    print("Press Ctrl+C to stop")
    print("="*80)
    
    # Change to dashboard directory
    os.chdir('/home/jovyan/work')
    
    # Run streamlit
    os.system('streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0 --server.headless true')