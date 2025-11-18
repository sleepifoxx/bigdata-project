#!/usr/bin/env python3
"""
PySpark-based HDFS uploader for CSV files
Upload CSV files from local data directory to HDFS using PySpark
Usage: python pyspark_upload_to_hdfs.py
"""

import os
import glob
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

class PySparkHDFSUploader:
    def __init__(self, hdfs_namenode="hdfs://namenode:9000"):
        self.hdfs_namenode = hdfs_namenode
        self.local_data_dir = "/home/jovyan/work/data"
        self.hdfs_input_dir = "/data/input"
        self.spark = None
        
    def initialize_spark(self):
        """Initialize Spark session with HDFS configuration"""
        print("🔧 Initializing Spark session...")
        
        try:
            conf = SparkConf().setAppName("HDFS CSV Uploader")
            conf.set("spark.hadoop.fs.defaultFS", self.hdfs_namenode)
            
            self.spark = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()
                
            print("✅ Spark session initialized successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize Spark session: {e}")
            return False
    
    def test_hdfs_connection(self):
        """Test HDFS connection"""
        print("🔍 Testing HDFS connection...")
        
        try:
            # Try to list HDFS root directory
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            
            # Try to create a simple path to test connectivity
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path("/")
            exists = fs.exists(path)
            
            print("✅ HDFS connection successful")
            return True
            
        except Exception as e:
            print(f"❌ HDFS connection failed: {e}")
            print(f"🔧 Check if HDFS namenode is running at {self.hdfs_namenode}")
            return False
    
    def create_hdfs_directory(self, hdfs_path):
        """Create directory in HDFS"""
        print(f"📂 Creating HDFS directory: {hdfs_path}")
        
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            
            # Create directory
            success = fs.mkdirs(path)
            
            if success or fs.exists(path):
                print(f"✅ Directory ready: {hdfs_path}")
                return True
            else:
                print(f"❌ Failed to create directory: {hdfs_path}")
                return False
                
        except Exception as e:
            print(f"❌ Error creating directory: {e}")
            return False
    
    def upload_csv_to_hdfs(self, local_csv_path, hdfs_dir):
        """Upload CSV file to HDFS using PySpark"""
        filename = os.path.basename(local_csv_path)
        # Don't include namenode in write path - Spark will use default FS
        hdfs_write_path = f"{hdfs_dir}/{filename}"
        
        print(f"📤 Uploading: {filename}")
        print(f"   Source: {local_csv_path}")
        print(f"   Target: {self.hdfs_namenode}{hdfs_write_path}")
        
        try:
            # Read CSV file from local filesystem (use file:// prefix)
            local_file_uri = f"file://{local_csv_path}"
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(local_file_uri)
            
            # Show basic info
            row_count = df.count()
            col_count = len(df.columns)
            print(f"   📊 Data: {row_count:,} rows, {col_count} columns")
            
            # Write to HDFS
            df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(f"{hdfs_write_path}_temp")
            
            # Rename the single part file to the desired filename
            self._rename_hdfs_file(f"{hdfs_write_path}_temp", hdfs_write_path)
            
            print(f"✅ Successfully uploaded: {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to upload {filename}: {e}")
            return False
    
    def _rename_hdfs_file(self, temp_dir, final_path):
        """Rename HDFS file from temp directory to final location"""
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            
            # Find the part file in temp directory
            temp_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_dir)
            files = fs.listStatus(temp_path)
            
            for file_status in files:
                if file_status.getPath().getName().startswith("part-"):
                    # Move the part file to final location
                    source_path = file_status.getPath()
                    target_path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(final_path)
                    fs.rename(source_path, target_path)
                    break
            
            # Clean up temp directory
            fs.delete(temp_path, True)
            
        except Exception as e:
            print(f"⚠️  File uploaded but rename failed: {e}")
    
    def list_hdfs_contents(self, hdfs_dir):
        """List contents of HDFS directory"""
        print(f"📋 Contents of HDFS directory: {hdfs_dir}")
        
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_dir)
            
            if fs.exists(path):
                files = fs.listStatus(path)
                for file_status in files:
                    name = file_status.getPath().getName()
                    size = file_status.getLen()
                    size_mb = size / (1024 * 1024)
                    print(f"   📄 {name} ({size_mb:.2f} MB)")
            else:
                print("   Directory doesn't exist")
                
        except Exception as e:
            print(f"Error listing HDFS contents: {e}")
    
    def get_file_info(self, filepath):
        """Get file size and info"""
        try:
            stat = os.stat(filepath)
            size_mb = stat.st_size / (1024 * 1024)
            return f"{size_mb:.2f} MB"
        except:
            return "Unknown size"
    
    def upload_all_csv_files(self):
        """Main method to upload all CSV files"""
        print("📁 PySpark HDFS CSV Uploader")
        print("============================")
        
        # Check if local data directory exists
        if not os.path.exists(self.local_data_dir):
            print(f"❌ Local data directory not found: {self.local_data_dir}")
            return False
        
        # Find CSV files
        csv_pattern = os.path.join(self.local_data_dir, "*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            print(f"❌ No CSV files found in {self.local_data_dir}")
            print("📁 Available files:")
            try:
                for file in os.listdir(self.local_data_dir):
                    filepath = os.path.join(self.local_data_dir, file)
                    if os.path.isfile(filepath):
                        size = self.get_file_info(filepath)
                        print(f"   {file} ({size})")
            except:
                print("   Directory is empty or inaccessible")
            return False
        
        print("📊 Found CSV files:")
        for csv_file in csv_files:
            size = self.get_file_info(csv_file)
            print(f"   {os.path.basename(csv_file)} ({size})")
        print()
        
        # Initialize Spark
        if not self.initialize_spark():
            return False
        
        # Test HDFS connection
        if not self.test_hdfs_connection():
            return False
        
        # Create HDFS directory
        if not self.create_hdfs_directory(self.hdfs_input_dir):
            return False
        
        # Upload files
        print("\n🚀 Starting upload process...")
        success_count = 0
        total_count = len(csv_files)
        
        for csv_file in csv_files:
            if self.upload_csv_to_hdfs(csv_file, self.hdfs_input_dir):
                success_count += 1
            print()
        
        # Summary
        print("📊 Upload Summary:")
        print("==================")
        print(f"✅ Successful uploads: {success_count}")
        print(f"📁 Total files: {total_count}")
        
        if success_count == total_count:
            print("🎉 All files uploaded successfully!")
        else:
            print("⚠️  Some files failed to upload")
        
        # Show HDFS contents
        print()
        self.list_hdfs_contents(self.hdfs_input_dir)
        
        print()
        print(f"🔗 HDFS NameNode: {self.hdfs_namenode}")
        print(f"📂 HDFS Path: {self.hdfs_input_dir}")
        
        # Close Spark session
        if self.spark:
            self.spark.stop()
            print("🔧 Spark session closed")
        
        return success_count == total_count

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print("""
📖 PySpark HDFS Upload Script Usage
===================================
This script uploads CSV files from local data directory to HDFS using PySpark

Prerequisites:
1. HDFS namenode must be accessible at hdfs://namenode:9000
2. CSV files must be available in /home/jovyan/work/data directory  
3. PySpark must be properly configured

Usage:
  python pyspark_upload_to_hdfs.py           # Upload all CSV files
  python pyspark_upload_to_hdfs.py --help    # Show this help

Environment:
  Source: /home/jovyan/work/data/*.csv (pyspark-notebook container)
  Target: hdfs://namenode:9000/data/input (HDFS)
        """)
        return
    
    uploader = PySparkHDFSUploader()
    success = uploader.upload_all_csv_files()
    
    if success:
        print("\n✅ Upload completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Upload completed with errors!")
        sys.exit(1)

if __name__ == "__main__":
    main()